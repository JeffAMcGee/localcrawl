import maroon
from maroon import *

from models import User, Tweet, as_local_id
from twitter import TwitterResource
from settings import settings

Model.database = CouchDB(settings.couchdb,True)
res = TwitterResource()
timeline = res.user_timeline()
print timeline[0].to_d()
#users = user_lookup([1224891])
