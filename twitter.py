from restkit import OAuthFilter, request, Resource, TConnectionManager
import restkit
import restkit.util.oauth2 as oauth
import json
import time
import logging
from datetime import datetime
from restkit.errors import RequestFailed, Unauthorized
from settings import settings
from models import Edges, User, Tweet
from couchdbkit import BulkSaveError


class TwitterResource(Resource):
    # When a request fails, we retry with an exponential backoff from
    # 15 to 240 seconds.
    backoff_seconds = [15*2**x for x in xrange(5)]

    def __init__(self):
        consumer = oauth.Consumer(
                key=settings.consumer_key,
                secret=settings.consumer_secret)
        token = oauth.Token(
                key=settings.token_key,
                secret=settings.token_secret)
        url = "http://api.twitter.com/1/"
        auth = OAuthFilter('*', consumer, token)
        mgr = TConnectionManager()
        Resource.__init__(self,
                url,
                filters=[auth],
                conn_manager=mgr,
                client_opts={'timeout':30}
        )
        self.remaining = 10000

    def get_d(self, path=None, headers=None, **kwargs):
        for delay in self.backoff_seconds:
            try:
                r = self.get(path, headers, **kwargs)
                if 'x-ratelimit-remaining' in r.headers:
                    self.remaining = int(r.headers['x-ratelimit-remaining'])
                    stamp = int(r.headers['x-ratelimit-reset'])
                    self.reset_time = datetime.utcfromtimestamp(stamp)
                if r.status_int == 304:
                    # I don't think this should happen - that's
                    # why I raise the exception.
                    raise Exception("Error 304 - %s, "%r.final_url)
                return json.loads(r.body_string())
            except ValueError:
                logging.exception("incomplete json")
            except RequestFailed as failure:
                if failure.response.status_int == 502:
                    logging.info("Fail whale says slow down!")
                else:
                    logging.error("%s while retrieving %s",
                        failure.response.status,
                        failure.response.final_url
                    )
                if failure.response.status_int in (400,420,503):
                    # The whale says slow WAY down!
                    delay = 240
            time.sleep(delay)
        raise Exception("Epic Fail Whale! - %s"%path)

    def get_ids(self, path, user_id, **kwargs):
        ids=self.get_d(
            path=path,
            user_id=user_id,
            cursor=-1,
            **kwargs
        )
        return (str(id) for id in ids['ids'])

    def user_lookup(self, user_ids=[], screen_names=[], **kwargs):
        ids = ','.join(str(u) for u in user_ids)
        names = ','.join(screen_names)
        lookup = self.get_d(
            "users/lookup.json",
            screen_name=names,
            user_id=ids,
            **kwargs
        )
        users = [User(d) for d in lookup]
        if len(users)==len(user_ids) or screen_names:
            return users
        # Ick. Twitter just removes suspended users from the results.
        d = dict((u._id,u) for u in users)
        return [d.get(str(uid),None) for uid in user_ids]

    def friends_ids(self, user_id):
        return self.get_ids("friends/ids.json", user_id)

    def followers_ids(self, user_id):
        return self.get_ids("followers/ids.json", user_id)

    def get_edges(self, user_id):
        return Edges(
                _id=user_id,
                friends=self.friends_ids(user_id),
                followers=self.followers_ids(user_id),
        )

    def user_timeline(self, user_id, count=200, **kwargs):
        timeline = self.get_d(
            "statuses/user_timeline.json",
            user_id=user_id,
            trim_user=1,
            include_rts=1,
            include_entities=1,
            count=count,
            **kwargs
        )
        return [Tweet(t) for t in timeline]

    def save_timeline(self, uid, last_tid, max_tid=None):
        since_id = int(last_tid)-1
        max_id = int(max_tid)-1 if max_tid else None

        all_tweets = []
        while since_id != max_id:
            try:
                tweets = self.user_timeline(
                    uid,
                    max_id = max_id,
                    since_id = since_id,
                    count = 100,
                )
            except Unauthorized:
                logging.warn("unauthorized!")
                break
            if not tweets:
                logging.warn("no tweets found after %d for %s",len(all_tweets),uid)
                break
            all_tweets+=tweets
            if len(tweets)<90:
                #there are no more tweets, and since_id+1 was deleted
                break
            max_id =int(tweets[-1]._id)-1
            if len(all_tweets)>=3150:
                logging.error("hit max tweets after %d for %s",len(all_tweets),uid)
                break
        try:
            Tweet.database.bulk_save_models(t for t in all_tweets if int(t._id)-1>since_id)
        except BulkSaveError as err:
            #ignore conflicts
            if any(d['error']!='conflict' for d in err.errors):
                raise
            else:
                logging.warn("conflicts for %s:",uid)
                logging.warn("%r",err.errors)
        return all_tweets
