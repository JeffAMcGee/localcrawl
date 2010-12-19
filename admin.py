#!/usr/bin/env python
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
import heapq
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


def import_json():
    #for l in f:
    #    try:
    #        db.save_doc(json.loads(l))
    #    except ResourceConflict:
    #        print "conflict for %s"%(l.strip())
    for g in grouper(1000,sys.stdin):
        db.bulk_save(json.loads(l) for l in g if l)


def export_json(start=None,end=None):
    for d in db.paged_view('_all_docs',include_docs=True,startkey=start,endkey=end):
        del d['doc']['_rev']
        print json.dumps(d['doc'])


def merge_db():
    names = (
        "hou_f1","hou_f1b","hou_f2","hou_f2b","hou_f3","hou_f4",
        "hou_f5","hou_f6","hou_f7","hou_f8","hou_f9","hou_lu",
    )
    views = [
        connect(name).paged_view('_all_docs',include_docs=True)
        for name in names
    ]
    last =None
    for row in merge_views(*views):
        if row['key']!=last:
            del row['doc']['_rev']
            print json.dumps(row['doc'])
            last = row['key']


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


def count_recent():
    min_int_id = 8000000000000000L
    view = db.paged_view('user/and_tweets')
    for k,g in itertools.groupby(view, lambda r:r['key'][0]):
            user_d = g.next()
            if user_d['id'][0] != 'U':
                print "fail %r"%user_d
                continue
            tweets = sum(1 for r in g if as_int_id(r['id'])>min_int_id)
            print "%d\t%s"%(tweets,user_d['id'])
         

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


def merge_views(*views):
    # This is based on heapq.merge in python 2.6.  The big difference is
    # that it sorts by key.
    h = []
    for itnum, it in enumerate(map(iter, views)):
        try:
            row = it.next()
            h.append([row['key'], itnum, row, it.next])
        except StopIteration:
            pass
    heapq.heapify(h)

    while 1:
        try:
            while 1:
                k, itnum, v, next = s = h[0]   # raises IndexError when h is empty
                yield v
                s[2] = next()               # raises StopIteration when exhausted
                s[0] = s[2]['key']
                heapq.heapreplace(h, s)          # restore heap condition
        except StopIteration:
            heapq.heappop(h)                     # remove empty iterator
        except IndexError:
            return


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
    local_db = CouchDB('http://127.0.0.1:5984/hou',True)
    local_view = local_db.paged_view('_all_docs',startkey='U',endkey='V')
    local_users = set(r['id'] for r in local_view)

    locs = (-1,0,.5,1)
    weights =(.1,.3,.5,.7,.9)
    counts = dict(
        (score, dict(
            (loc, dict(
                (weight,0)
                for weight in weights))
            for loc in locs))
        for score in xrange(BUCKETS))
    

    for user in all_users():
        if user['doc'].get('utco')!=-21600:
            continue
        state, rfs, ats = scores.split(as_int_id(user['id']))
        if user['id'] in local_users:
            loc = 1
        else:
            try:
                loc = .5 if user['doc']['prob']==.5 else 0
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


def force_lookup(to_db="hou",start_id='',end_id=None):
    "Lookup users who were not included in the original crawl."
    start ='U'+start_id
    end = 'U'+end_id if end_id else 'V'
    user_view = db.paged_view('_all_docs',include_docs=True,startkey=start,endkey=end)
    users = (User(d['doc']) for d in user_view)
    Model.database = connect(to_db)
    scores = Scores()
    scores.read(settings.lookup_out)
    for user in users:
        int_uid = as_int_id(user._id)
        if user.lookup_done or user.protected or int_uid not in scores: continue
        if user.local_prob!=0 or user.geonames_place.name in ("Texas","United States"):
            continue
        state, rfs, ats = scores.split(int_uid)
        #FIXME: this is a one-off thing to get the highly-connected non-locals
        if user.utc_offset != -21600 or log_score(rfs,ats) < settings.non_local_cutoff:
            continue
        tweets = res.save_timeline(user._id,last_tid=settings.min_tweet_id)
        if not tweets: continue
        user.last_tid = tweets[0]._id
        user.last_crawl_date = dt.utcnow()
        user.next_crawl_date = dt.utcnow()
        user.tweets_per_hour = settings.tweets_per_hour
        user.lookup_done = True
        user.attempt_save()
        logging.info("saved %d from %s to %s",len(tweets),tweets[-1]._id,tweets[0]._id)
        sleep_if_needed()


def sleep_if_needed():
    logging.info("api calls remaining: %d",res.remaining)
    if res.remaining < 10:
        delta = (res.reset_time-dt.utcnow())
        logging.info("goodnight for %r",delta)
        time.sleep(delta.seconds)


def fix_doc_type():
    for t in db.view('fix/doc_type',include_docs=True):
        t['doc']['doc_type']='Tweet'
        db.save_doc(t['doc'])


def fill_800(start='U',end='U2'):
    users = db.paged_view('_all_docs',include_docs=True,startkey=start,endkey=end)
    unknown = set(u['id'] for u in users if u['doc']['prob']!=1)
    print "done making unknown set"
    view = db.paged_view('once/near_800',
        startkey=start,
        endkey=end,
        group=True,
        stale="ok",
    )
    count = 0
    for row in view:
        fore,aft = row['value']
        if aft==None:
            continue
        if row['key'] not in unknown:
            continue
        tweets = res.save_timeline(
            row['key'],
            last_tid=settings.min_tweet_id,
            max_tid=as_local_id('T',aft) if aft else None,
        )
        logging.info("saved %d for %s",len(tweets),row['key'])
        count = 0 if tweets else count+1
        if count==100: return
        sleep_if_needed()
 

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
    if os.environ.get('COUCH'):
        db = connect(os.environ['COUCH'])
        Model.database = db
    if len(sys.argv)>1:
        locals()[sys.argv[1]](*sys.argv[2:])
