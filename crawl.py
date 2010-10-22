from collections import defaultdict
import beanstalkc
import json
import pdb
import signal
from couchdbkit import ResourceConflict 

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings


RFRIEND_POINTS = 1000
MENTION_POINTS = 1000

signal.signal(signal.SIGINT, lambda x, y: pdb.set_trace())

class UserCrawler():
    def __init__(self):
        self.res = TwitterResource()
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        self.stalk.use('score')
        self.stalk.watch('lookup')

    def _local_guess(self,user):
        "this is really stupid for now"
        loc = user.location.lower()
        for here in ('college station','bryan','aggieland'):
            if here in loc:
                return 1.0
        for here in ('austin','dallas'):
            if here in loc:
                return 0.0
        return .5

    def crawl(self):
        while True:
            # reserve blocks to wait when x is 0, but returns None for 1-19
            jobs = [self.stalk.reserve(0 if x else None) for x in xrange(20)]
            bodies = [JobBody.from_job(j) for j in jobs if j is not None]
            users =self.res.user_lookup([b._id for b in bodies])

            print "looking at %r"%[u.screen_name for u in users]
            #get user_ids from beanstalk
            for job,body,user in zip(jobs,bodies,users):
                try:
                    print "look at %s"%user.screen_name
                    user.local.rfriends_score = body.rfriends_score
                    user.local.mention_score = body.mention_score
                    self.crawl_user(user)
                    #FIXME - calc daily_tweets
                    user.local.daily_tweets = -1.0
                    user.attempt_save()
                    job.delete()
                except Exception as ex:
                    print ex
                    pdb.post_mortem()
                    job.bury()

    def crawl_user(self,user):
        user.local.local_prob = self._local_guess(user)
        if user.local.local_prob == 0 or user.protected:
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
        
        if user.local.local_prob == 1.0:
            self.store_new_users(user, rels, tweets)

    def store_new_users(self, user, rels, tweets):
        jobs = defaultdict(JobBody)

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
    Model.database = CouchDB(settings.couchdb,True)

    #jeffamcgee and aggieastronaut
    #users = ['U106582358','U17560063']
    crawler = UserCrawler()
    crawler.crawl()
    #tweets = crawler.res.user_timeline('U106582358')

