from restkit import OAuthFilter, request, Resource, SimplePool
import restkit
import restkit.util.oauth2 as oauth
import json
import time
import logging
from restkit.errors import RequestFailed
from settings import settings


def create_twitter_resource():
    consumer = oauth.Consumer(
            key=settings.consumer_key,
            secret=settings.consumer_secret)
    token = oauth.Token(
            key=settings.token_key,
            secret=settings.token_secret)
    url ="http://api.twitter.com/1/"
    auth = OAuthFilter('*', consumer, token)
    pool = SimplePool(keepalive=2)
    return TwitterResource(url, filters=[auth], pool_instance=pool)


class TwitterResource(Resource):
    # When a request fails, we retry with an exponential backoff from
    # 15 to 240 seconds.
    backoff_seconds = [15*2**x for x in xrange(5)]

    def get_d(self, path=None, headers=None, **kwargs):
        for delay in self.backoff_seconds:
            try:
                r = self.get(path, headers, **kwargs)
                self.remaining = r.headers['x-ratelimit-remaining']
                if r.status_int == 304:
                    # I don't think this should happen - that's
                    # why I raise the exception.
                    raise Exception("Error 304 - %s, "%r.final_url)
                return json.loads(r.body_string())
            except RequestFailed as failure:
                print failure.response.status_int
                print failure.response.final_url
                logging.error("%s while retrieving %s",
                        failure.response.status,
                        failure.response.final_url
                )
                if r.status_int in (400,420,502,503):
                    # The whale says slow down!
                    delay = 240
                time.sleep(delay)
        raise Exception("Epic Fail Whale! - %s"%r.final_url)
