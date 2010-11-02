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


#signal.signal(signal.SIGINT, lambda x, y: pdb.set_trace())

class UserCrawler():
    def __init__(self):
        self.res = TwitterResource()
        self.stalk = beanstalkc.Connection(
                settings.beanstalk_host,
                settings.beanstalk_port,
                )
        self.stalk.use('crawled')
        self.stalk.watch('crawl')

    def crawl(self):
        while True:
            try:
                job = self.stalk.reserve(None)
                d = self._crawl_job(json.loads(job.body))
                self.stalk.put(json.dumps(d),ttr=settings.beanstalk_ttr)
                job.delete()

                if self.res.remaining < 10:
                    dt = (self.res.reset_time-datetime.utcnow())
                    print "goodnight for %r"%dt
                    time.sleep(dt.seconds)
            except Exception as ex:
                print ex
                job.bury()
                pdb.post_mortem()
            print "api calls remaining: %d"%self.res.remaining

    def _crawl_job(self, job):
        uid = job['uid']
        since_id = as_int_id(job['since_id'])-1
        print "crawl %s"%uid

        count = 0
        max_id = None
        while count<=3000 and since_id != max_id:
            tweets = self.res.user_timeline(
                uid,
                max_id = max_id,
                since_id = since_id,
            )
            count+=len(tweets)
            max_id =as_int_id(tweets[-1]._id)-1
            for tweet in tweets:
                if as_int_id(tweet._id)-1>since_id:
                    tweet.save()
            if not len(tweets):
                print "no tweets found!"

        print "saved %d"%count
        return dict(
            _id = uid,
            count = count,
        )

if __name__ == '__main__':
    Model.database = CouchDB(settings.couchdb,True)
    crawler = UserCrawler()
    crawler.crawl()


