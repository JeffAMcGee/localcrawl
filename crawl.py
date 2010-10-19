import maroon
from maroon import *

from models import User, Tweet, as_local_id
from twitter import TwitterResource
from settings import settings

Model.database = CouchDB(settings.couchdb,True)
res = TwitterResource()
timeline = res.get_tweets(
    "statuses/user_timeline.json",
    screen_name='kourt17',
    count=100,
)
print timeline[0].to_d()
#users = user_lookup([1224891])
