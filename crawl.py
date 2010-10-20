from collections import defaultdict
import beanstalkc
import json
import pdb

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings


RFRIEND_POINTS = 1000
MENTION_POINTS = 1000


class UserCrawler():
    def __init__(self):
        self.res = TwitterResource()
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        self.stalk.use('scores')
        self.stalk.watch('users')

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
            jobs = [self.stalk.reserve() for x in xrange(1)]
            bodies = [JobBody(json.loads(j.body)) for j in jobs]
            users =self.res.user_lookup([b._id for b in bodies])
            
            #get user_ids from beanstalk
            for job,body,user in zip(jobs,bodies,users):
                try:
                    print "look at %s"%user.screen_name
                    self.crawl_user(user)
                    user.local.rfriends_score = body.rfriends_score
                    user.local.mention_score = body.mention_score
                    #FIXME - calc daily_tweets
                    user.local.daily_tweets = -1.0
                    user.save()
                    job.delete()
                except Exception as ex:
                    print ex
                    pdb.post_mortem()
                    job.bury()

    def crawl_user(self,user):
        user.local.local_prob = self._local_guess(user)
        if user.local.local_prob == 0:
            return
        ats = defaultdict(int)
        rfriends = []
        if user.followers_count>0 and user.friends_count>0:
            rels = self.res.get_relationships(user._id)
            rels.save()
            rfriends = rels.rfriends()
        if user.statuses_count>0 and not user.protected:
            tweets = self.res.user_timeline(user._id)
            for tweet in tweets:
                tweet.save()
                for uid in tweet.mentions:
                    ats[uid]+=1
        
        if user.local.local_prob == 1.0:
            self.store_new_users(user, rfriends, ats)

    def store_new_users(self, user, rfriends, ats):
        jobs = defaultdict(JobBody)
        if len(rfriends) < RFRIEND_POINTS:
            for u in rfriends:
               jobs[u].rfriends_score = RFRIEND_POINTS/len(rfriends)
        at_count = sum(ats.values())
        for u,c in ats.iteritems():
            jobs[u].mention_score = c*MENTION_POINTS/at_count
        for k,j in jobs.iteritems():
            j._id = k
            self.stalk.put(json.dumps(j.to_d()))


if __name__ == '__main__':
    Model.database = CouchDB(settings.couchdb,True)

    #jeffamcgee and aggieastronaut
    #users = ['U106582358','U17560063']
    crawler = UserCrawler()
    crawler.crawl()
    #tweets = crawler.res.user_timeline('U106582358')

