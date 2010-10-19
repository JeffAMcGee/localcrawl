import maroon
from maroon import *

from models import User, Tweet, as_local_id
from twitter import create_twitter_resource
from settings import settings

Model.database = CouchDB(settings.couchdb,True)
res = create_twitter_resource()

def user_lookup(user_ids):
    lookup = res.get_d(
        "users/lookup.json",
        user_id=','.join(str(u) for u in user_ids)
    )
    ret = []
    for d in lookup:
        user = User(d)
        ret.append(user)
    return ret


def user_timeline():
    timeline = res.get_d(
        "statuses/user_timeline.json",
        trim_user=1,
        include_rts=1,
        include_entities=1,
        screen_name='localboter',
    )
    tweets = [Tweet(status) for status in timeline]
    for tweet in tweets:
        tweet._id = tweet.id
        tweet.mentions = [
            as_local_id('U',at['id'])
            for at in tweet.entities['user_mentions']]
    return tweets


timeline = user_timeline()
#users = user_lookup([1224891])
