# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.

from couchdbkit.loaders import FileSystemDocsLoader
from models import *
import json
import beanstalkc
from settings import settings
from collections import defaultdict
import twitter

db = CouchDB(settings.couchdb,True)
res = twitter.TwitterResource()
Model.database = db
c = beanstalkc.Connection()

def connect(name):
    Model.database = CouchDB('http://127.0.0.1:5984/'+name,True)
    return Model.database

def design_sync():
    loader = FileSystemDocsLoader('_design')
    loader.sync(db, verbose=True)

def clear(tube):
    c.watch(tube)
    while True:
        j = c.reserve(0)
        if j is None:
            return
        print j.body
        j.delete()

def couch_import(path):
    data = json.load(open(path))
    for row in data['rows']:
        d = row['doc']
        del d['_rev']
        db.save_doc(d)

def print_counts():
    counts = defaultdict(lambda: defaultdict(int))

    res = db.view('user/all',include_docs=True)
    for d in res:
        for k,v in d['doc'].iteritems():
            if k != 'l' and not isinstance(v,list):
                counts[k][v]+=1
        for k,v in d['doc']['l'].iteritems():
            if not isinstance(v,list):
                counts[k][v]+=1

    for k,d in counts.iteritems():
        print k
        for v in sorted(d.keys()):
            print "%d\t%r"%(d[v],v)
        print

def rm_next_crawl():
    latest = db.view('user/latest',group=True)
    latest = set(l['key'] for l in latest)
    for user in db.paged_view('user/next_crawl',include_docs=True):
        if user['id'] not in latest:
            del user['doc']['l']['ncd']
            db.save_doc(user['doc'])

def make_jeff():
    jeff = CouchDB('http://127.0.0.1:5984/jeff',True)
    for row in db.paged_view('user/and_tweets',include_docs=True):
        if row['key'][0][-2:] == '58':
            jeff.save_doc(row['doc'])
