from collections import defaultdict
import beanstalkc
import json
import pdb
import signal
from couchdbkit import ResourceConflict 
from datetime import datetime
import time
from itertools import groupby

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings
from gisgraphy import GisgraphyResource


RFRIEND_POINTS = 1000
MENTION_POINTS = 1000

#signal.signal(signal.SIGINT, lambda x, y: pdb.set_trace())

class UserCrawler():
    def __init__(self):
        self.res = TwitterResource()
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        self.stalk.use('score')
        self.stalk.watch('lookup')
        self.gisgraphy = GisgraphyResource()

    def _guess_location(self,user):
        if not user.location:
            return .5
        place = self.gisgraphy.twitter_loc(user.location)
        if not place:
            return .5
        user.geonames_place = place
        return 1 if self.gisgraphy.in_local_box(place.to_d()) else 0

    def crawl(self):
        while True:
            # reserve blocks to wait when x is 0, but returns None for 1-19
            jobs = (self.stalk.reserve(0 if x else None) for x in xrange(20))
            jobs = [j for j in jobs if j is not None]
            bodies = [JobBody.from_job(j) for j in jobs]
            users =self.res.user_lookup([b._id for b in bodies])

            print "looking at %r"%[u.screen_name for u in users]
            #get user_ids from beanstalk
            for job,body,user in zip(jobs,bodies,users):
                try:
                    if self.res.remaining < 30:
                        dt = (self.res.reset_time-datetime.utcnow())
                        print "goodnight for %r"%dt
                        time.sleep(dt.seconds)
                    print "look at %s"%user.screen_name
                    if user._id in User.database:
                        job.delete()
                        continue
                    self.crawl_user(user)
                    user.rfriends_score = body.rfriends_score
                    user.mention_score = body.mention_score
                    user.tweets_per_hour = settings.tweets_per_hour
                    user.save()
                    job.delete()
                except Exception as ex:
                    print ex
                    pdb.post_mortem()
                    job.bury()
            print "api calls remaining: %d"%self.res.remaining

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

    #def fixup(self):
        #view = Model.database.paged_view('user/screen_name',include_docs=True)
        #for user in (User(d['doc']) for d in view):
            #user.save()


if __name__ == '__main__':
    Model.database = CouchDB(settings.couchdb,True)
    crawler = UserCrawler()
    crawler.fixup()
    #crawler.crawl()


