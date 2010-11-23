from collections import defaultdict
import beanstalkc
import json
import pdb
import signal
from datetime import datetime, timedelta
import time
from itertools import groupby
from restkit import Unauthorized
import logging
from multiprocessing import Queue

import maroon
from maroon import *

from models import Relationships, User, Tweet, JobBody, as_local_id, as_int_id
from twitter import TwitterResource
from settings import settings
from gisgraphy import GisgraphyResource
from procs import LocalProc, create_slaves


HALT = False
def set_halt(x=None,y=None):
    print "halting"
    global HALT
    HALT=True
signal.signal(signal.SIGINT, set_halt)
signal.signal(signal.SIGUSR1, set_halt)


class CrawlMaster(LocalProc):
    def __init__(self):
        LocalProc.__init__(self,"crawl")
        self.waiting = set()
        self.todo = Queue()
        self.done = Queue()

    def run(self):
        print "started crawl"
        logging.info("started crawl")
        try:
            while not HALT:
                self.queue_crawl()
        except:
            logging.exception("exception caused HALT")
        self.todo.close()
        self.todo.join()
        print "done"

    def queue_crawl(self):
        logging.info("queue_crawl")
        now = datetime.utcnow().timetuple()[0:6]
        for user in User.database.paged_view('user/next_crawl',endkey=now):
            uid = user['id']
            if HALT: break
            if uid in self.waiting: continue # they are queued
            self.waiting.add(uid)
            self.todo.put(uid)
            
            if len(self.waiting)%100==0:
                read_crawled()
                # let the queue empty a bit

    def read_crawled(self):
        logging.info("read_crawled, %d",len(self.waiting))
        try:
            while True:
                uid = self.done.get_nowait()
                self.waiting.remove(uid)
        except Queue.Empty:
            return


class CrawlSlave(LocalProc):
    def __init__(self, slave_id, todo, done):
        LocalProc.__init__(self,'crawl', slave_id)
        self.res = TwitterResource()
        self.todo = todo
        self.done = done

    def run(self):
        while True:
            user=None
            try:
                uid = self.todo.get()
                user = User.get_id(uid)
                self.crawl(user)
                self.done.put(uid)
                self.todo.task_done()
                if self.res.remaining < 10:
                    dt = (self.res.reset_time-datetime.utcnow())
                    logging.info("goodnight for %r",dt)
                    time.sleep(dt.seconds)
            except Exception as ex:
                if user:
                    logging.exception("exception for user %s"%user.to_d())
                else:
                    logging.exception("exception and user is None")
            logging.info("api calls remaining: %d",self.res.remaining)

    def crawl(self, user):
        since_id = as_int_id(user.last_tid)-1
        logging.debug("visiting %s - %s",user._id,user.screen_name)

        count = 0
        max_id = None
        last_tid = None
        while since_id != max_id:
            try:
                tweets = self.res.user_timeline(
                    user._id,
                    max_id = max_id,
                    since_id = since_id,
                )
            except Unauthorized:
                logging.warn("unauthorized!")
                break
            if not tweets:
                logging.warn("no tweets found after %d for %s",count,user._id)
                break
            if not last_tid:
                last_tid = tweets[0]._id
            count+=len(tweets)
            max_id =as_int_id(tweets[-1]._id)-1
            for tweet in tweets:
                if as_int_id(tweet._id)-1>since_id:
                    #FIXME: replace with save??
                    tweet.attempt_save()
            if len(tweets)<175:
                #there are no more tweets, and since_id+1 was deleted
                break
            if count>=3100:
                logging.error("hit max tweets after %d for %s",count,user._id)
                break
        self.update(user,count,last_tid)

    def update(self,user,count,last_tid):
        if last_tid:
            user.last_tid = last_tid
        now = datetime.utcnow()
        delta = now - user.last_crawl_date
        seconds = delta.seconds + delta.days*24*3600
        tph = (3600.0*count/seconds + user.tweets_per_hour)/2
        user.tweets_per_hour = tph
        hours = min(settings.tweets_per_crawl/tph, settings.max_hours)
        user.next_crawl_date = now+timedelta(hours=hours)
        user.last_crawl_date = now
        user.save()


if __name__ == '__main__':
    proc = CrawlMaster()
    create_slaves(CrawlSlave, proc.todo, proc.done)
    proc.run()
