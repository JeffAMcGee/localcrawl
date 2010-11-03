import json
import signal
import time
from math import log
import sys
import traceback
from datetime import datetime, timedelta

import beanstalkc
from couchdbkit import ResourceConflict 

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings
from scoredict import Scores, log_score, BUCKETS
import scoredict

CRAWL_PROPORTION = .10


HALT = False
def set_halt(x=None,y=None):
    global HALT
    HALT=True
signal.signal(signal.SIGINT, set_halt)
signal.signal(signal.SIGUSR1, set_halt)


class Brain():
    def __init__(self):
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        self.scores = Scores()
        self.lookups = 0

    def lookup(self):
        self.stalk.use('lookup')
        self.stalk.watch('score')
        while True:
            print "read_scores"
            self.read_scores()
            #FIXME: it stops at 10000 scores for debuging
            if HALT or len(self.scores)>10000:
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
        self.stats = [0 for x in xrange(BUCKETS)]
        for u in self.scores:
            state, rfs, ats = self.scores.split(u)
            if state==scoredict.NEW:
                self.stats[log_score(rfs,ats)]+=1
        for count,score in zip(self.stats,xrange(BUCKETS)):
            print "%d %d"%(score,count)
        total = 0
        for i in xrange(BUCKETS-1,-1,-1):
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

    def crawl(self):
        self.stalk.use('crawl')
        self.stalk.watch('crawled')
        waiting = dict()
        try:
            while not HALT:
                print "queue_crawl"
                self.queue_crawl(waiting)
                print "read_crawled, %d"%len(waiting)
                self.read_crawled(waiting)
        except:
                traceback.print_exc()
        while waiting:
            print "read_crawled after HALT, %d"%len(waiting)
            try:
                self.read_crawled(waiting)
            except:
                traceback.print_exc()

    def queue_crawl(self, waiting):
        now = datetime.utcnow().timetuple()[0:6]
        view = Model.database.paged_view('user/next_crawl', endkey=now)
        for user in view:
            if len(waiting)>500: return # let the queue empty a bit
            uid = user['id']
            if uid in waiting: continue # they are queued
            latest = Model.database.view('user/latest', key=uid)
            if not len(latest): continue # they have never tweeted
            value = latest.one()['value']
            waiting[uid] = datetime(*value[1])
            d = dict(uid=uid,since_id=value[0])
            self.stalk.put(json.dumps(d))


    def read_crawled(self, waiting):
        job = self.stalk.reserve(60)
        while job is not None:
            d = json.loads(job.body)
            print d
            user = User.get_id(d['uid'])
            now = datetime.utcnow()
            delta = now - waiting[user._id]
            seconds = delta.seconds + delta.days*24*3600
            tph = (3600.0*d['count']/seconds + user.tweets_per_hour)/2
            user.tweets_per_hour = tph
            hours = min(settings.tweets_per_crawl/tph, settings.max_hours)
            user.next_crawl_date = now+timedelta(hours=hours)
            del waiting[user._id]
            user.save()
            job.delete()
            job = self.stalk.reserve(60)


if __name__ == '__main__':
    Model.database = CouchDB(settings.couchdb,True)
    brain = Brain()
    if sys.argv[1] == 'lookup':
        brain.scores.read(settings.brain_in)
        brain.lookups = brain.scores.count_lookups()
        brain.lookup()
    elif sys.argv[1] == 'crawl':
        brain.crawl()
    else:
        print "brain.py [lookup|crawl]"

