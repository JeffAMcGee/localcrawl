
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
        
        Model.database = CouchDB(settings.couchdb_root+settings.region,True)

        log = label+"_"+slave_id if slave_id else label
        filepath = os.path.join(settings.log_dir, log)
        logging.basicConfig(filename=filepath,level=logging.INFO)


def _run_slave(Proc,slave_id):
    p = Proc(slave_id)
    p.run()


def create_slaves(Proc):
    for x in xrange(settings.slaves):
        slave_id = string.letters[x]
        p = Process(target=_run_slave, args=(Proc,slave_id,))
        p.start()