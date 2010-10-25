# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.

from couchdbkit.loaders import FileSystemDocsLoader
from models import *
import beanstalkc
from settings import settings

db = CouchDB(settings.couchdb,True)
Model.database = db
c = beanstalkc.Connection()

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

