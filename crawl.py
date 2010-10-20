from collections import defaultdict

import maroon
from maroon import *

from models import Relationships, User, Tweet, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings

class UserCrawler():
    def __init__(self):
        self.res = TwitterResource()

    def _local_guess(self,user):
        "this is really stupid for now"
        loc = user.location.lower()
        for here in ('college station','bryan','aggieland'):
            if here in loc:
                return 1.0
        for here in ('houston','austin','dallas'):
            if here in loc:
                return 0.0
        return .5

    def crawl(self,user_ids):
        #get user_ids from beanstalk
        for user in self.res.user_lookup(user_ids):
            self.crawl_user(user)
            user.save()
            #talk to beanstalkd

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
        self.store_new_users(self, user, rfriends, ats)

    def store_new_users(self, user, rfriends, ats)
        pass


if __name__ == '__main__':
    Model.database = CouchDB(settings.couchdb,True)

    #jeffamcgee and aggieastronaut
    users = ['U106582358','U17560063']
    crawler = UserCrawler()
    crawler.crawl(users)
    #tweets = crawler.res.user_timeline('U106582358')

