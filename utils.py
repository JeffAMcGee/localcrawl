# This is a tool for testing and administrative tasks.  It is designed to
# be %run in ipython.

from models import *
import json
import beanstalkc
from settings import settings
import time
import os,errno
import logging
from datetime import datetime as dt


class LocalApp(object):
    def __init__(self, task, slave_id=""):
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        label = settings.region+"_"+task
        self.stalk.watch(label if slave_id else label+"_done")
        self.stalk.use(label+"_done" if slave_id else label)
        
        Model.database = CouchDB(settings.couchdb_root+settings.region,True)

        log = label+"_"+slave_id if slave_id else label
        filepath = os.path.join(settings.log_dir, log)
        logging.basicConfig(filename=filepath)
        
