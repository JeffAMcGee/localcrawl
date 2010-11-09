from restkit import OAuthFilter, request, Resource, SimplePool
import restkit
import restkit.util.oauth2 as oauth
import json
import time
import logging
from datetime import datetime
from restkit.errors import RequestFailed
from settings import settings
from models import Relationships, User, Tweet, as_local_id, as_int_id


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
        pool = SimplePool(keepalive=2)
        Resource.__init__(self,
                url,
                filters=[auth],
                pool_instance=pool,
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
            user_id=as_int_id(user_id),
            **kwargs
        )
        return (as_local_id('U',id) for id in ids)

    def user_lookup(self, user_ids, **kwargs):
        ids = ','.join(str(as_int_id(u)) for u in user_ids)
        lookup = self.get_d(
            "users/lookup.json",
            user_id=ids,
            **kwargs
        )
        return [User(d) for d in lookup]

    def friends_ids(self, user_id):
        return self.get_ids("friends/ids.json", user_id)

    def followers_ids(self, user_id):
        return self.get_ids("followers/ids.json", user_id)

    def get_relationships(self, user_id):
        return Relationships(
                _id=as_int_id(user_id),
                friends=self.friends_ids(user_id),
                followers=self.followers_ids(user_id),
        )

    def user_timeline(self, user_id, **kwargs):
        timeline = self.get_d(
            "statuses/user_timeline.json",
            user_id=as_int_id(user_id),
            trim_user=1,
            include_rts=1,
            include_entities=1,
            count=200,
            **kwargs
        )
        return [Tweet(t) for t in timeline]
