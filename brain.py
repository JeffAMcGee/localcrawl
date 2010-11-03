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

    def analyze(self):
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
            state, rfs, ats = self.scores.split(as_int_id(user._id))
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

    def force_lookup(self):
        #ratio of locals to non-locals taken from a spreadsheet
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
        endkey = datetime.utcnow().timetuple()[0:6]
        view = Model.database.paged_view('user/next_crawl', endkey=endkey)
        for user in view:
            if len(waiting)>100:
                return
            uid = user['id']
            if uid in waiting:
                continue
            latest = Model.database.view('user/latest', key=uid)
            if not len(latest):
                continue # we only pull tweets from users who have tweeted before
            value = latest.one()['value']
            waiting[uid] = datetime(*value[1])
            d = dict(uid=uid,since_id=value[0])
            self.stalk.put(json.dumps(d))


    def read_crawled(self, waiting):
        while True:
            job = self.stalk.reserve(60)
            if job is None:
                return
            d = json.loads(job.body)
            print d
            user = User.get_id(d['uid'])
            count = d['count']
            now = datetime.utcnow()
            delta = now - waiting[user._id]
            seconds = delta.seconds + delta.days*24*3600
            tph = (3600.0*count/seconds + user.tweets_per_hour)/2
            user.tweets_per_hour = tph
            hours = min(settings.tweets_per_crawl/tph, settings.max_hours)
            user.next_crawl_date = now+timedelta(hours=hours)
            del waiting[user._id]
            user.save()
            job.delete()


if __name__ == '__main__':
    Model.database = CouchDB(settings.couchdb,True)
    brain = Brain()
    brain.scores.read(settings.brain_in)
    brain.lookups = brain.scores.count_lookups()
    if sys.argv[1] == 'lookup':
        brain.lookup()
    elif sys.argv[1] == 'analyze':
        brain.analyze()
    elif sys.argv[1] == 'force':
        brain.force()
    elif sys.argv[1] == 'crawl':
        brain.crawl()
    else:
        print "brain.py [lookup|prep|crawl]"

