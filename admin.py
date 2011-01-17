#!/usr/bin/env python
# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.  If you import it from another module, you're doing
# something wrong.

import json
import itertools
import time
import logging
import heapq
import sys
import getopt
from collections import defaultdict
from datetime import datetime as dt

from couchdbkit import ResourceNotFound, BulkSaveError
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


def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)


def design_sync(type):
    "sync the documents in _design"
    loader = FileSystemDocsLoader(type+'_design')
    loader.sync(db, verbose=True)


def stop_lookup():
    stalk.use(settings.region+"_lookup_done")
    stalk.put('halt',0)


def import_json():
    for g in grouper(1000,sys.stdin):
        try:
            db.bulk_save([json.loads(l) for l in g if l])
        except BulkSaveError as err:
            if any(d['error']!='conflict' for d in err.errors):
                raise
            else:
                logging.warn("conflicts for %r",[d['id'] for d in err.errors])


def import_old_json():
    for g in grouper(1000,sys.stdin):
        docs = [json.loads(l) for l in g if l]
        for d in docs:
            del d['doc_type']
            for k,v in d.iteritems():
                if k[-2:]=='id' or k in ('rtt','rtu'):
                    d[k]=v[1:]
            for field in ['ats','fols','frs']:
                if field in d and isinstance(d[field],list):
                    d[field] = [u[1:] for u in d[field]]
        db.bulk_save(docs)


def export_json(start=None,end=None):
    for d in db.paged_view('_all_docs',include_docs=True,startkey=start,endkey=end):
        del d['doc']['_rev']
        print json.dumps(d['doc'])


def merge_db(*names,**kwargs):
    views = [
        connect(name).paged_view(
            '_all_docs',
            include_docs=True,
            startkey=kwargs.get('start'),
            endkey=kwargs.get('end'))
        for name in names
    ]
    last =None
    for row in merge_views(*views):
        if row['key']!=last:
            del row['doc']['_rev']
            print json.dumps(row['doc'])
            last = row['key']


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


def force_lookup(to_db="hou",start_id='',end_id=None):
    "Lookup users who were not included in the original crawl."
    start ='U'+start_id
    end = 'U'+end_id if end_id else 'V'
    user_view = db.paged_view('_all_docs',include_docs=True,startkey=start,endkey=end)
    users = (User(d['doc']) for d in user_view)
    Model.database = connect(to_db)
    found_db = connect("houtx")
    found_view = found_db.paged_view('_all_docs',startkey=start,endkey=end)
    found = set(d['id'] for d in found_view)
    scores = Scores()
    scores.read(settings.lookup_out)
    region = ("Texas","United States")
    for user in users:
        int_uid = as_int_id(user._id)
        if (    user.lookup_done or
                user.protected or
                int_uid not in scores or
                user.local_prob==1 or
                (user.local_prob==0 and user.geonames_place.name not in region) or
                user._id in found
           ):
            continue
        state, rfs, ats = scores.split(int_uid)
        if user.utc_offset == -21600:
            if log_score(rfs,ats,.9) < 1: continue
        else:
            if log_score(rfs,ats) < settings.non_local_cutoff: continue
        user_lookup(user)


def stdin_lookup():
    from_db = connect("orig_houtx")
    for l in sys.stdin:
        user = from_db.get_id(User,l.strip())
        user_lookup(user)


def user_lookup(user):
    tweets = res.save_timeline(user._id,last_tid=settings.min_tweet_id)
    if not tweets: return
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


if __name__ == '__main__' and len(sys.argv)>1:
    try:
        opts, args = getopt.getopt(sys.argv[2:], "c:s:e:")
    except getopt.GetoptError, err:
        print str(err)
        print "usage: ./admin.py function_name [-c database] [-s startkey] [-e endkey] [arguments]"
        sys.exit(2)
    kwargs={}
    for o, a in opts:
        if o == "-c":
            db = connect(a)
            Model.database = db
        elif o == "-s":
            kwargs['start']=a
        elif o == "-e":
            kwargs['end']=a
        else:
            assert False, "unhandled option"
    locals()[sys.argv[1]](*args,**kwargs)
