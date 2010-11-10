from collections import defaultdict
import beanstalkc
import json
import pdb
import signal
from datetime import datetime
import time
from itertools import groupby
import logging
import sys

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings
from gisgraphy import GisgraphyResource
from procs import LocalProc, create_slaves
from scoredict import Scores, log_score, BUCKETS
import scoredict


RFRIEND_POINTS = 1000
MENTION_POINTS = 1000


class LookupMaster(LocalProc):
    def __init__(self):
        LocalProc.__init__(self,"lookup")
        self.scores = Scores()
        self.scores.read(settings.lookup_in)
        self.lookups = self.scores.count_lookups()
        self.halt = False

    def run(self):
        print "starting lookup"
        logging.info("started lookup")
        try:
            while not self.halt:
                if self.should_pick():
                    logging.info("calc_cutoff")
                    cutoff = self.calc_cutoff()

                    if cutoff==0:
                        self.halt=True
                        print "halt because cutoff is 0"
                        break
                    logging.info("pick_users with score %d", cutoff)
                    self.pick_users(cutoff)
                    print "scores:%d lookups:%d"%(len(self.scores),self.lookups)

                logging.info("read_scores")
                self.read_scores()
        except:
            logging.exception("exception caused HALT")
        self.read_scores()
        self.scores.dump(settings.lookup_out)
        print "Lookup is done!"

    def read_scores(self):
        job = None
        stop = 10000000 if self.halt else 100000
        for x in xrange(stop):
            try:
                job = self.stalk.reserve(600)
                if job is None:
                    logging.info("loaded %d scores",x)
                    return
                if job.body=="halt":
                    self.halt=True
                    print "starting to halt..."
                    logging.info("starting to halt...")
                    job.delete()
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
            except:
                logging.exception("exception in read_scores caused HALT")
                self.halt = True
                if job:
                    job.bury()
                return

    def calc_cutoff(self):
        self.stats = [0 for x in xrange(BUCKETS)]
        for u in self.scores:
            state, rfs, ats = self.scores.split(u)
            if state==scoredict.NEW:
                self.stats[log_score(rfs,ats)]+=1
        for count,score in zip(self.stats,xrange(BUCKETS)):
            logging.info("%d %d",score,count)
        total = 0
        for i in xrange(BUCKETS-1,-1,-1):
            total+=self.stats[i]
            if total > settings.crawl_ratio*(len(self.scores)-self.lookups):
                return i
        return 0

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

    def should_pick(self):
        ready = self.stalk.stats_tube(self.stalk.using())['current-jobs-ready']
        logging.info("ready is %d",ready)
        return ready < settings.crawl_ratio*(len(self.scores)-self.lookups)


class LookupSlave(LocalProc):
    def __init__(self,slave_id):
        LocalProc.__init__(self,'lookup',slave_id)
        self.res = TwitterResource()
        self.gisgraphy = GisgraphyResource()

    def _guess_location(self,user):
        if not user.location:
            return .5
        place = self.gisgraphy.twitter_loc(user.location)
        if not place:
            return .5
        user.geonames_place = place
        return 1 if self.gisgraphy.in_local_box(place.to_d()) else 0

    def run(self):
        while True:
            # reserve blocks to wait when x is 0, but returns None for 1-19
            jobs = (self.stalk.reserve(0 if x else None) for x in xrange(20))
            jobs = [j for j in jobs if j is not None]
            bodies = [JobBody.from_job(j) for j in jobs]
            users =self.res.user_lookup([b._id for b in bodies])

            logging.info("looking at %r"%[u.screen_name for u in users])
            #get user_ids from beanstalk
            for job,body,user in zip(jobs,bodies,users):
                try:
                    if self.res.remaining < 30:
                        dt = (self.res.reset_time-datetime.utcnow())
                        logging.info("goodnight for %r",dt)
                        time.sleep(dt.seconds)
                    logging.info("look at %s",user.screen_name)
                    if user._id in User.database:
                        job.delete()
                        continue
                    self.crawl_user(user)
                    user.rfriends_score = body.rfriends_score
                    user.mention_score = body.mention_score
                    user.save()
                    job.delete()
                except:
                    logging.exception("exception for job %s"%job.body)
                    job.bury()
            logging.info("api calls remaining: %d",self.res.remaining)

    def crawl_user(self,user):
        user.local_prob = self._guess_location(user)
        if user.local_prob != 1.0 or user.protected:
            return
        rels=None
        tweets=None
        if user.followers_count>0 and user.friends_count>0:
            rels = self.res.get_relationships(user._id)
            rels.attempt_save()

        if user.statuses_count>0:
            tweets = self.res.user_timeline(user._id)
            for tweet in tweets:
                tweet.attempt_save()
        if tweets:
            user.next_crawl_date = datetime.utcnow()
            user.tweets_per_hour = settings.tweets_per_hour
        
        user.lookup_done = True
        if user.local_prob == 1.0:
            self.score_new_users(user, rels, tweets)

    def score_new_users(self, user, rels, tweets):
        jobs = defaultdict(JobBody)
        jobs[user._id].done = True

        if rels:
            rfriends = rels.rfriends()
            if len(rfriends) < RFRIEND_POINTS:
                for u in rfriends:
                   jobs[u].rfriends_score = RFRIEND_POINTS/len(rfriends)

        if tweets:
            ats = defaultdict(int)
            for tweet in tweets:
                for uid in tweet.mentions:
                    ats[uid]+=1
            at_count = sum(ats.values())
            for u,c in ats.iteritems():
                points = c*MENTION_POINTS/at_count
                if points >0:
                    jobs[u].mention_score = points

        for k,j in jobs.iteritems():
            j._id = k
            j.put(self.stalk)


if __name__ == '__main__':
    if len(sys.argv) >1:
        if sys.argv[1]=='m':
            proc = LookupMaster()
        elif sys.argv[1]=='s':
            proc = LookupSlave('x')
    else:
        print "spawning minions!"
        create_slaves(LookupSlave)
        proc = LookupMaster()
    proc.run()
