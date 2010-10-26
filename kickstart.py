# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.

from couchdbkit.loaders import FileSystemDocsLoader
from models import *
import json
import beanstalkc
from settings import settings

db = CouchDB(settings.couchdb,True)
Model.database = db
#c = beanstalkc.Connection()

def reload_design():
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
