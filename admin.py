# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.  If you import it from another module, you're doing
# something wrong.

import json
import itertools
import time
import os,errno
import logging
import random
import gzip
import sys
from collections import defaultdict
from datetime import datetime as dt
from operator import itemgetter

from couchdbkit import ResourceNotFound
from couchdbkit.loaders import FileSystemDocsLoader
import beanstalkc

from settings import settings
import twitter
from models import *
from gisgraphy import GisgraphyResource
from scoredict import Scores, BUCKETS, log_score
import lookup

gisgraphy = GisgraphyResource()
db = CouchDB(settings.couchdb_root+settings.region,True)
res = twitter.TwitterResource()
Model.database = db
logging.basicConfig(level=logging.INFO)

try:
    stalk = beanstalkc.Connection()
except:
    pass


def connect(name):
    return CouchDB(settings.couchdb_root+name,True)


def design_sync():
    "sync the documents in _design"
    loader = FileSystemDocsLoader('_design')
    loader.sync(db, verbose=True)


def stop_lookup():
    stalk.use(settings.region+"_lookup_done")
    stalk.put('halt',0)


def all_users():
    return db.paged_view('_all_docs',include_docs=True,startkey='U',endkey='V')


def all_tweets():
    return db.paged_view('_all_docs',include_docs=True,startkey='T',endkey='U')


def count_users(key):
    counts = defaultdict(int)
    for u in all_users():
        counts[u['doc'].get(key,None)]+=1
    for k in sorted(counts.keys()):
        print "%r\t%d"%(k,counts[k])


def count_locations(path='counts'):
    counts = defaultdict(int)
    for u in all_users():
        if u['doc'].get('prob',0)==1:
            loc = u['doc'].get('loc',"")
            norm = " ".join(re.split('[^0-9a-z]+', loc.lower())).strip()
            counts[norm]+=1
    f = open(path,'w')
    for k,v in sorted(counts.iteritems(),key=itemgetter(1)):
        print>>f, "%r\t%d"%(k,v)
    f.close()


def import_gz(path):
    f = gzip.GzipFile(path)
    for l in f:
        try:
            db.save_doc(json.loads(l))
        except ResourceConflict:
            print "conflict for %s"%(l.strip())
    #for g in grouper(1000,f):
        #db.bulk_save(json.loads(l) for l in g if l)
    f.close()


def export_gz(path):
    f = gzip.GzipFile(path,'w',1)
    for d in db.paged_view('_all_docs',include_docs=True):
        del d['doc']['_rev']
        print >>f,json.dumps(d['doc'])
    f.close()


def set_latest_from_view(path="mmt.json"):
    for row in db.view('user/latest',group=True):
        try:
            user = User.get_id(row['key'])
            user.last_tid = row['value'][0]
            user.last_crawl_date = dt(2010,11,12)
            user.save()
        except ResourceNotFound:
            pass

def set_latest(path="mmt.json"):
    """Fill in last_tid based on the latest view. This
    code will only be used once per database."""
    for l in open(path):
        row = json.loads(l)
        try:
            user = User.get_id(row['uid'])
            user.last_tid = row['max_id']
            user.last_crawl_date = dt(2010,11,12)
            user.save()
        except ResourceNotFound:
            pass


def fake_lu_master():
    proc = lookup.LookupMaster()
    while not proc.halt:
        proc.read_scores()
        print "scores:%d"%len(proc.scores)
    print "halting"
    proc.read_scores()
    proc.scores.dump(settings.lookup_out)


def fake_lu_slave():
    proc = lookup.LookupSlave('y')
    Relationships.database = CouchDB('http://127.0.0.1:5984/orig_houtx',True)
    view = db.paged_view('dev/user_and_tweets',include_docs=True)
    for k,g in itertools.groupby(view, lambda r:r['key'][0]):
        user_d = g.next()
        if user_d['id'][0] != 'U':
            print "fail %r"%user_d
            continue
        user = User(user_d['doc'])
        print "scoring %s - %s"%(user._id, user.screen_name)
        tweets = [Tweet(r['doc']) for r in g]
        if user.local_prob != 1.0:
            continue
        try:
            rels = Relationships.get_for_user_id(user._id)
        except ResourceNotFound:
            print "rels not found"
            rels = None
        proc.score_new_users(user, rels, tweets)
    print "done"


def make_jeff_db():
    """Make a subset of 1% of the database - user docs and tweets for users
    whose user_id ends in 58 (i.e. @JeffAMcGee)"""
    jeff = CouchDB('http://127.0.0.1:5984/jeff',True)
    for row in db.paged_view('user/and_tweets',include_docs=True):
        if row['key'][0][-2:] == '58':
            jeff.save_doc(row['doc'])


def copy_locals(path='hou_ids',to_db='hou'):
    User.database = connect(to_db)
    f = open(path,'w')
    for user in (User(d['doc']) for d in all_users()):
        if user.local_prob==1:
            user.save()
            print >>f, user._id
    f.close()


def copy_tweets(path='hou_ids',dbname='hou'):
    out_db = CouchDB('http://127.0.0.1:5984/'+dbname,True)
    locals = set(l.strip() for l in open(path))
    for t in all_tweets():
        if as_int_id(t['id'])>27882000000 and t['doc']['uid'] in locals:
            del t['doc']['_rev']
            out_db.save_doc(t['doc'])


def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)


def count_sn(path):
    "used to evaluate the results of localcrawl"
    lost =0
    found =0
    sns = (sn.strip() for sn in open(path))
    for group in grouper(20,sns):
        for user in res.user_lookup([], screen_name=','.join(group)):
            if user._id in db:
                found+=1
                print "found %s - %s"%(user.screen_name,user._id)
            else:
                lost+=1
                print "missed %s - %s"%(user.screen_name,user._id)
    print "lost:%d found:%d"%(lost,found)

def analyze():
    "Find out how the scoring algorithm did."
    scores = Scores()
    scores.read(settings.lookup_out)
    local_view = db.paged_view('_all_docs',startkey='U',endkey='V')
    local_users = set(r['id'] for r in local_view)

    locs = (-1,0,.5,1)
    weights =(0,.25,.5,.75,1)
    counts = dict(
        (score, dict(
            (loc, dict(
                (weight,0)
                for weight in weights))
            for loc in locs))
        for score in xrange(BUCKETS))
    
    user_db = CouchDB('http://127.0.0.1:5984/orig_houtx',True)
    for int_id in scores:
        state, rfs, ats = scores.split(int_id)
        uid = as_local_id('U',int_id)
        if uid in local_users:
            loc = 1
        else:
            try:
                user = user_db.get(uid)
                loc = .5 if user['prob']==.5 else 0
            except ResourceNotFound:
                loc = -1

        for weight in weights:
            score = log_score(rfs,ats,weight)
            counts[score][loc][weight]+=1

    print "todo\t\t\t\t\tnon\t\t\t\t\tunk\t\t\t\t\tlocal"
    for score in xrange(BUCKETS):
        for loc in locs:
            for weight in weights:
                print "%d\t"%counts[score][loc][weight],
        print


def force_lookup():
    "Lookup users who were not included in the original crawl."
    # FIXME: ratio of locals to non-locals taken from a spreadsheet - it
    # should come from analyze()!
    probs = [.02,.02,.03,.04,.08,.13,.22,.32,.47,.69,.67,.88,.83,1,1]
    res = TwitterResource()
    for user in (User(d['doc']) for d in all_users()):
        if user.local_prob != 1:
            score = log_score(user.rfriends_score, user.mention_score)
            if( user.local_prob==.5
                and score >= settings.force_cutoff
                and not user.lookup_done
            ):
                user.tweets_per_hour = settings.tweets_per_hour
                user.next_crawl_date = dt.utcnow()
                user.lookup_done = True
                for tweet in res.user_timeline(user._id):
                    tweet.attempt_save()
            user.local_prob = probs[score]
            user.save()


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as ex:
        if ex.errno!=errno.EEXIST:
            raise


def min_max_tid(path):
    view = Model.database.paged_view(
            'tweet/uid',
            include_docs=True,
        )
    with open(path,'w') as f:
        for k,g in itertools.groupby(view,itemgetter('key')):
            l = list(g)
            min_tweet = min(l,key=lambda x: as_int_id(x['id']))['doc']
            max_tweet = max(l,key=lambda x: as_int_id(x['id']))['doc']
            print>>f,json.dumps(dict(
                uid = k,
                count = len(l),
                min_ca = min_tweet['ca'],
                min_id = min_tweet['_id'],
                max_ca = max_tweet['ca'],
                max_id = max_tweet['_id'],
            ))


def krishna_export(start=[2010],end=None):
    "export the tweets for Krishna's crawler"
    view = Model.database.paged_view(
            'tweet/date',
            include_docs=True,
            startkey=start,
            endkey=end
        )
    for k,g in itertools.groupby(view,itemgetter('key')):
        path = os.path.join(*(str(x) for x in k))
        mkdir_p(os.path.dirname(path))
        with open(path,'w') as f:
            for t in (row['doc'] for row in g):
                ts = int(time.mktime(dt(*t['ca']).timetuple()))
                if t['ats']:
                    for at in t['ats']:
                        print>>f,"%d %s %s %s"%(ts,t['_id'],t['uid'],at)
                else:
                    print>>f,"%d %s %s"%(ts,t['_id'],t['uid'])

if __name__ == '__main__':
    if len(sys.argv)>1:
        locals()[sys.argv[1]](*sys.argv[2:])
