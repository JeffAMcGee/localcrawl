from collections import defaultdict
import beanstalkc
import json
import pdb
import signal
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
from procs import LocalProc, create_slaves


HALT = False
def set_halt(x=None,y=None):
    global HALT
    HALT=True
signal.signal(signal.SIGINT, set_halt)
signal.signal(signal.SIGUSR1, set_halt)


class CrawlMaster(LocalProc):
    def __init__(self):
        LocalProc.__init__(self,"crawl")

    def run(self):
        print "started crawl"
        logging.info("started crawl")
        waiting = dict()
        try:
            while not HALT:
                logging.info("queue_crawl")
                self.queue_crawl(waiting)
                logging.info("read_crawled, %d",len(waiting))
                self.read_crawled(waiting)
        except:
            logging.exception("exception caused HALT")
            set_halt()
        while waiting:
            logging.info("read_crawled after HALT, %d",len(waiting))
            try:
                self.read_crawled(waiting)
            except:
                logging.exception("exception after HALT")

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
            logging.debug(d)
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



#signal.signal(signal.SIGINT, lambda x, y: pdb.set_trace())

class CrawlSlave(LocalProc):
    def __init__(self,slave_id):
        LocalProc.__init__(self,'crawl',slave_id)
        self.res = TwitterResource()

    def run(self):
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
    create_slaves(LookupSlave)
    proc = LookupMaster()
    proc.run()
