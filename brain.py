import json
import signal
import time
from math import log

import beanstalkc
from couchdbkit import ResourceConflict 

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings
from scoredict import Scores, log_score
import scoredict

CRAWL_PROPORTION = .10


HALT = False
def set_halt(x,y):
    global HALT
    HALT=True
signal.signal(signal.SIGINT, set_halt)
signal.signal(signal.SIGUSR1, set_halt)


class CrawlController():
    def __init__(self):
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        self.stalk.use('lookup')
        self.stalk.watch('score')
        self.scores = Scores()
        self.lookups = 0

    def launch(self):
        while True:
            print "read_scores"
            self.read_scores()
            if HALT or len(self.scores)>100000:
                self.scores.dump(settings.brain_out)
                return
            print "calc_cutoff"
            cutoff = self.calc_cutoff()
            print "pick_users with score %d"%cutoff
            self.pick_users(cutoff)
            print "wait"
            while self.should_wait() and not HALT:
                time.sleep(60)

    def read_scores(self):
        for x in xrange(1000000):
            job = self.stalk.reserve(0)
            if job is None:
                print "loaded %d"%x
                return
            body = JobBody.from_job(job)
            if body.done:
                self.scores.set_state(as_int_id(body._id), scoredict.DONE)
            else:
                self.scores.increment(
                    as_int_id(body._id),
                    body.rfriends_score,
                    body.mention_score
                )
            job.delete()

    def calc_cutoff(self):
        buckets = 1+int(log(scoredict.MAX_SCORE,2))
        self.stats = [0 for x in xrange(buckets)]
        for u in self.scores:
            state, rfs, ats = self.scores.split(u)
            if state==scoredict.NEW:
                self.stats[log_score(rfs,ats)]+=1
        for count,score in zip(self.stats,xrange(buckets)):
            print "%d %d"%(score,count)
        total = 0
        for i in xrange(buckets-1,-1,-1):
            total+=self.stats[i]
            if total > settings.crawl_ratio*(len(self.scores)-self.lookups):
                return i

    def pick_users(self, cutoff):
        for uid in self.scores:
            state, rfs, ats = self.scores.split(uid)
            if state==scoredict.NEW and log_score(rfs,ats) >= cutoff:
                job = JobBody(
                    _id=as_local_id('U',uid),
                    rfriends_score=rfs,
                    mention_score=ats,
                )
                job.put(self.stalk)
                self.scores.set_state(uid, scoredict.LOOKUP)
                self.lookups+=1

    def should_wait(self):
        ready = self.stalk.stats_tube('lookup')['current-jobs-ready']
        print "ready is %d"%ready
        return ready > settings.crawl_ratio*(len(self.scores)-self.lookups)


if __name__ == '__main__':
    Model.database = CouchDB(settings.couchdb,True)
    controller = CrawlController()
    controller.scores.read(settings.brain_in)
    controller.launch()

