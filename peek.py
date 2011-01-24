#!/usr/bin/env python
# This is a tool for investigating the collected data.  It is designed to
# be %run in ipython.  If you import it from another module, you're doing
# something wrong.

import itertools
import time
import os
import logging
import sys
import getopt
import math
from collections import defaultdict
from datetime import datetime as dt
from operator import itemgetter

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.transforms import Bbox
import numpy

from couchdbkit import ResourceNotFound

from settings import settings
import twitter
from models import *
from maroon import ModelCache
from scoredict import Scores, BUCKETS, log_score


db = CouchDB(settings.couchdb_root+settings.region,True)
res = twitter.TwitterResource()
Model.database = db
logging.basicConfig(level=logging.INFO)


def connect(name):
    return CouchDB(settings.couchdb_root+name,True)


def all_users():
    return db.paged_view('_all_docs',include_docs=True,endkey='_')


def place_tweets(start, end):
    return db.paged_view('tweet/plc',
            include_docs=True,
            startkey=None,
            endkey=None,
            startkey_docid=start,
            endkey_docid=end,
            )


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


def count_tweets_in_box(start='T',end='U'):
    counts = defaultdict(int)
    box = settings.local_box
    for row in place_tweets(start,end):
        if 'coord' in row['doc']:
            c = row['doc']['coord']['coordinates']
            if box['lng'][0]<c[0]<box['lng'][1] and box['lat'][0]<c[1]<box['lat'][1]:
                counts['inb']+=1
            else:
                counts['outb']+=1
        else:
            counts['noco']+=1
    print dict(counts)


def print_locs(start='T',end='U'):
    for row in place_tweets(start,end):
        if 'coord' in row['doc']:
            c = row['doc']['coord']['coordinates']
            print '%f\t%f\t%s'%(c[0],c[1],row['doc']['uid'])


def read_locs(path=None):
    for l in open(path or "locs_uid"):
        s = l.split()
        yield float(s[0]),float(s[1]),s[2]
    logging.info("read points")


def plot_tweets():
    #usage: peek.py print_locs| peek.py plot_tweets
    locs = read_locs()
    mid_x,mid_y = (-95.4,29.8)
    box = settings.local_box
    lngs,lats = zip(*[
            c[0:2] for c in locs
            #if math.hypot(c[0]-mid_x,c[1]-mid_y)<10
            #if -96<c[0]<-94.6 and 29.2<c[1]<30.4
            ])
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cmap = LinearSegmentedColormap.from_list("gray_map",["#c0c0c0","k","k"])
    ax.hexbin(lngs,lats,
            gridsize=2000,
            bins='log',
            cmap=cmap,
            mincnt=1,
            linewidth=.0001,
            )
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    #ax.set_title("Tweets from Houston, TX (11/26/2010-1/14/2010)")
    fig.savefig('../www/world.pdf')


def dist_histogram():
    mid_x,mid_y = (-95.4,29.8)
    locs = read_locs()
    dists = [math.hypot((loc[0]-mid_x),loc[1]-mid_y) for loc in locs]
    dists = [x for x in dists if x<5]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.hist(dists,bins=100)
    fig.savefig('../www/hist.png')


def user_stddev(path=None):
    means = []
    locs = sorted(read_locs(path),key=itemgetter(2))
    for k,g in itertools.groupby(locs,key=itemgetter(2)):
        lats,lngs = zip(*[x[0:2] for x in g])
        if len(lats)==1: continue
        m_lat,m_lng = numpy.mean(lats),numpy.mean(lngs)
        dists = (math.hypot(m_lat-x,m_lng-y) for x,y in zip(lats,lngs))
        mean = sum(dists)/len(lats)
        #if mean<2:
        means.append(70*mean)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.hist(means,bins=100,log=False)
    fig.savefig('../www/user.png')
    print numpy.median(means)


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
         

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)


def count_sn(path):
    "used to evaluate the results of localcrawl"
    lost =0
    found =0
    sns = (sn.strip() for sn in open(path))
    for group in grouper(100,sns):
        for user in res.user_lookup([], screen_names=group):
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


def _triangle_set():
    db = connect('houtx_user')
    users = db.paged_view('_all_docs',include_docs=True,endkey="_")
    for row in users:
        user = row['doc']
        if user['prot'] or user['prob']==.5:
            continue
        if user['frdc']>2000 and user['folc']>2000:
            continue
        if user['prob']==0 or row['id'][-1]=="8":
            yield int(row['id'])


def find_tris():
    Edges.database = connect("houtx_edges")
    users = set(_triangle_set())
    logging.info("looking at %d users",len(users))
    edges = {}
    for uid in tuple(users):
        try:
            obj = Edges.get_id(str(uid))
        except ResourceNotFound:
            users.remove(uid)
        edges[uid] = users.intersection(
                (int(f) for f in obj.friends if int(f)>uid),
                (int(f) for f in obj.followers if int(f)>uid),
                )
    for me in users:
        for friend in edges[me]:
            amigos = edges[me].intersection(edges[friend])
            for amigo in amigos:
                print " ".join(str(id) for id in (me, friend, amigo))


def coord_in_miles(p1, p2):
    return math.hypot(69.1*(p1.lat-p2.lat), 60.4*(p1.lng-p2.lng))


def tri_legs():
    User.database = connect("houtx_user")
    users = ModelCache(User)
    mins = []
    maxs = []
    for line in open("tris"):
        tri = [users[id].geonames_place for id in line.split()]
        legs = zip(tri,tri[1:]+tri[:1])
        dists = sorted(coord_in_miles(*leg) for leg in legs)
        if dists[2]<20:
            mins.append(dists[0])
            maxs.append(dists[2])
        #if len(mins)>1000: break
        #print "%f %f"%(dists[0],dists[2])
    logging.info("read points")
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cmap = LinearSegmentedColormap.from_list("gray_map",["#c0c0c0","k"])
    ax.hexbin(mins,maxs,
            gridsize=500,
            clip_box=Bbox.from_bounds(0,0,1000,1000),
            clip_on=True,
            bins="log",
            cmap=cmap,
            mincnt=1,
            linewidth=.0001,
            )
    fig.savefig('../www/tri_hou.png')


def rfriends():
    db = connect('houtx_edges')
    edges = db.paged_view('_all_docs',include_docs=True)
    clowns = 0
    nobodys = 0
    for row in edges:
        d = row['doc']
        if len(d['frs'])>2000 and len(d['fols'])>2000:
            clowns+=1
            continue
        rfs = set(d['frs']).intersection(d['fols'])
        if not rfs:
            nobodys+=1
            continue
        print " ".join([d['_id']]+list(rfs))
    logging.info("clowns: %d",clowns)
    logging.info("nobodys: %d",nobodys)

if __name__ == '__main__' and len(sys.argv)>1:
    try:
        opts, args = getopt.getopt(sys.argv[2:], "c:")
    except getopt.GetoptError, err:
        print str(err)
        print "usage: ./peek.py function_name [-c database] [arguments]"
        sys.exit(2)
    for o, a in opts:
        if o == "-c":
            db = connect(a)
            Model.database = db
        else:
            assert False, "unhandled option"
    locals()[sys.argv[1]](*args)
