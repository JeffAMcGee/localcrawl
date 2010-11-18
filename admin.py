# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.  If you import it from another module, you're doing
# something wrong.

import json
import itertools
import time
import os,errno
import logging
import gzip
import sys
from collections import defaultdict
from datetime import datetime
from operator import itemgetter

from couchdbkit.loaders import FileSystemDocsLoader
import beanstalkc

from settings import settings
import twitter
from models import *
from gisgraphy import GisgraphyResource
from scoredict import Scores, BUCKETS, log_score

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
    "connect to the couchdb database on localhost named name"
    Model.database = CouchDB('http://127.0.0.1:5984/'+name,True)
    return Model.database


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
        db.save_doc(json.loads(l))
    f.close()


def export_gz(path):
    f = gzip.GzipFile(path,'w',1)
    for d in db.paged_view('_all_docs',include_docs=True):
        del d['doc']['_rev']
        print >>f,json.dumps(d['doc'])
    f.close()


def rm_next_crawl():
    """Remove the next_crawl_date field if it shouldn't be there.  This
    code is no longer needed."""
    latest = db.view('user/latest',group=True)
    latest = set(l['key'] for l in latest)
    for user in db.paged_view('user/next_crawl',include_docs=True):
        if user['id'] not in latest:
            del user['doc']['ncd']
            db.save_doc(user['doc'])


def make_jeff_db():
    """Make a subset of 1% of the database - user docs and tweets for users
    whose user_id ends in 58 (i.e. @JeffAMcGee)"""
    jeff = CouchDB('http://127.0.0.1:5984/jeff',True)
    for row in db.paged_view('user/and_tweets',include_docs=True):
        if row['key'][0][-2:] == '58':
            jeff.save_doc(row['doc'])


def copy_locals():
    scores = Scores()
    scores.read(settings.lookup_out)
    User.database = CouchDB('http://127.0.0.1:5984/hou',True)
    for user in (User(d['doc']) for d in all_users()):
        if user.local_prob==1:
            if strictly_local(user.location):
                state, rfs, ats = scores.split(as_int_id(user._id))
                user.rfriends_score = rfs
                user.mention_score = ats
                user.save()
            else:
                print "ignoring '%s'"%user.location


def copy_tweets(input='hou_ids',dbname='hou'):
    out_db = CouchDB('http://127.0.0.1:5984/'+dbname,True)
    locals = set(l.strip() for l in open(input))
    for t in all_tweets():
        if as_int_id(t['id'])>27882000000 and t['doc']['uid'] in locals:
            del t['doc']['_rev']
            out_db.save_doc(t['doc'])


def strictly_local(loc):
    place = gisgraphy.twitter_loc(loc,True)
    if place.name=='Sugar Land':
        if 'sugar' not in loc.lower():
            return False
    return gisgraphy.in_local_box(place.to_d())

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)


def count_sn(path):
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
    locs = (0,.5,1)
    weights =(0,settings.mention_weight,1)
    counts = dict(
        (score, dict(
            (loc, dict(
                (weight,0)
                for weight in weights))
            for loc in locs))
        for score in xrange(BUCKETS))

    for user in (User(d['doc']) for d in all_users()):
        user.local_prob
        state, rfs, ats = scores.split(as_int_id(user._id))
        user.rfriends_score = rfs
        user.mention_score = ats
        #user.save()
        if user.local_prob in locs:
            for weight in weights:
                score = log_score(rfs,ats,weight)
                counts[score][user.local_prob][weight]+=1

    print "\tnon\t\t\tunk\t\t\tlocal"
    print "\trfs\tavg\tats\trfs\tavg\tats\trfs\tavg\tats\ttot"
    for score in xrange(BUCKETS):
        print score,
        for loc in locs:
            for weight in weights:
                print "\t%d"%counts[score][loc][weight],
        print "\t%d"%sum(
            counts[score][loc][settings.mention_weight]
            for loc in locs)


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
                user.next_crawl_date = datetime.utcnow()
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


def random_tweets():
    '''this creates test tweets for testing couchdb'''
    lorem = (
        'lorem ipsum dolor sit amet, consectetur adipiscing elit. curabitur id '+
        'malesuada augue. etiam lobortis mauris nec enim pretium id luctus sed.'
    )

    rand = couchdb('http://127.0.0.1:5984/rand',True)
    seq = CouchDB('http://127.0.0.1:5984/seq',True)
    flat = open('tweets.json','w')
    ids = [i**3 for i in xrange(10)]
    random.shuffle(ids)
    for tid in ids:
        t = Tweet(
            mentions = ['U123456789','U1248163264'],
            geo = (-95.123,25.367),
            created_at = dt.now(),
            favorited = False,
            text = LOREM[0:random.randint(20,140)],
            user_id = 'U106582358', #@Jeffamcgee
        )
        t.tweet_id=tid
        seq.save(t)
        t._id=tid
        t.tweet_id=None
        rand.save(t)
        print >>flat,json.dumps(t.to_d())


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
                ts = int(time.mktime(datetime(*t['ca']).timetuple()))
                if t['ats']:
                    for at in t['ats']:
                        print>>f,"%d %s %s %s"%(ts,t['_id'],t['uid'],at)
                else:
                    print>>f,"%d %s %s"%(ts,t['_id'],t['uid'])

if __name__ == '__main__':
    if len(sys.argv)>1:
        locals()[sys.argv[1]](*sys.argv[2:])
