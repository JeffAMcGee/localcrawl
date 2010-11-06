# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.

import json
import itertools
import time
import os,errno
import logging
from collections import defaultdict
from datetime import datetime
from operator import itemgetter

from couchdbkit.loaders import FileSystemDocsLoader
import beanstalkc

from settings import settings
import twitter
from models import *

db = CouchDB(settings.couchdb_root+settings.region,True)
res = twitter.TwitterResource()
Model.database = db
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


def print_counts():
    "determine how unique a key is - don't try to use this on a big dataset!"
    counts = defaultdict(lambda: defaultdict(int))

    res = db.view('user/screen_name',include_docs=True)
    for d in res:
        for k,v in d['doc'].iteritems():
            if not isinstance(v,list):
                counts[k][v]+=1

    for k,d in counts.iteritems():
        print k
        for v in sorted(d.keys()):
            print "%d\t%r"%(d[v],v)
        print

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


def rm_local():
    """Move everything in the local property into the user.  This code is no
    longer needed."""
    for user in db.paged_view('user/screen_name',include_docs=True):
        if 'l' in user['doc']:
            user['doc'].update(user['doc']['l'])
            del user['doc']['l']
            db.save_doc(user['doc'])


def analyze():
    "Find out how the scoring algorithm did."
    scores = Scores()
    scores.read(settings.brain_in)
    locs = (0,.5,1)
    weights =(0,settings.mention_weight,1)
    counts = dict(
        (score, dict(
            (loc, dict(
                (weight,0)
                for weight in weights))
            for loc in locs))
        for score in xrange(BUCKETS))

    view = Model.database.paged_view('user/screen_name',include_docs=True)
    for user in (User(d['doc']) for d in view):
        user.local_prob
        state, rfs, ats = scores.split(as_int_id(user._id))
        user.rfriends_score = rfs
        user.mention_score = ats
        user.save()
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
    view = Model.database.paged_view('user/screen_name',include_docs=True)
    res = TwitterResource()
    for user in (User(d['doc']) for d in view):
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
