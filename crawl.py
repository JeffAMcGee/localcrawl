from collections import defaultdict
import beanstalkc
import json
import pdb
import signal
from couchdbkit import ResourceConflict 
from datetime import datetime
import time
from itertools import groupby
from restkit import Unauthorized
import logging

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings
from gisgraphy import GisgraphyResource
from utils import LocalApp


#signal.signal(signal.SIGINT, lambda x, y: pdb.set_trace())

class UserCrawler(LocalApp):
    def __init__(self,slave_id):
        LocalApp.__init__(self,'crawl',slave_id)
        self.res = TwitterResource()

    def crawl(self):
        while True:
            job=None
            try:
                job = self.stalk.reserve(None)
                d = self._crawl_job(job)
                self.stalk.put(json.dumps(d),ttr=settings.crawl_ttr)
                job.delete()

                if self.res.remaining < 10:
                    dt = (self.res.reset_time-datetime.utcnow())
                    logging.info("goodnight for %r",dt)
                    time.sleep(dt.seconds)
            except Exception as ex:
                if job:
                    logging.exception("exception for job %s"%job.body)
                    job.bury()
                else:
                    logging.exception("exception and job is None")
            logging.info("api calls remaining: %d",self.res.remaining)

    def _crawl_job(self, job):
        d = json.loads(job.body)
        uid = d['uid']
        since_id = as_int_id(d['since_id'])-1
        logging.debug("%r",d)

        count = 0
        max_id = None
        while count<=3000 and since_id != max_id:
            try:
                tweets = self.res.user_timeline(
                    uid,
                    max_id = max_id,
                    since_id = since_id,
                )
            except Unauthorized:
                logging.warn("unauthorized!")
                break
            if not tweets:
                logging.warn("no tweets found after %d for %s",count,uid)
                break
            job.touch()
            count+=len(tweets)
            max_id =as_int_id(tweets[-1]._id)-1
            for tweet in tweets:
                if as_int_id(tweet._id)-1>since_id:
                    #FIXME: replace with save??
                    tweet.attempt_save()
            if len(tweets)<175:
                #there are no more tweets, and since_id+1 for was deleted
                break

        return dict(uid=uid, count=count)

if __name__ == '__main__':
    crawler = UserCrawler('a')
    crawler.crawl()
