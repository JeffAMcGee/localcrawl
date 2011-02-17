
import beanstalkc
import os
import string
import logging
from multiprocessing import Process

from settings import settings
from models import *


class LocalProc(object):
    def __init__(self, task, slave_id=""):
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        label = settings.region+"_"+task
        self.stalk.watch(label if slave_id else label+"_done")
        self.stalk.use(label+"_done" if slave_id else label)
        
        if settings.db == 'couch':
            Model.database = CouchDB(settings.couchdb_root+settings.region,True)
        else:
            Model.database = MongoDB(name=settings.region)

        log = label+"_"+slave_id if slave_id else label
        filepath = os.path.join(settings.log_dir, log)
        logging.basicConfig(filename=filepath,level=logging.INFO)


def _run_slave(Proc,slave_id,*args):
    p = Proc(slave_id,*args)
    try:
        p.run()
    except:
        logging.exception("exception killed proc")
        print "exception killed proc"


def create_slaves(Proc,*args):
    for x in xrange(settings.slaves):
        slave_id = string.letters[x]
        run_args = (Proc,slave_id)+args
        p = Process(target=_run_slave, args=run_args)
        p.start()
