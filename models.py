import datetime
import maroon
from maroon import *


def as_local_id(prefix,id):
    return "%c%d"%(prefix,id)


def as_int_id(id):
    return int(id[1:])


class TwitterModel(Model):
    def __init__(self, from_dict=None, **kwargs):
        Model.__init__(self, from_dict, **kwargs)
        if self._id is None and 'id' in from_dict:
            self._id = from_dict['id']


class TwitterIdProperty(TextProperty):
    def __init__(self, name, prefix, **kwargs):
        TextProperty.__init__(self, name, **kwargs)
        self._prefix = prefix

    def validated(self, val):
        val = Property.validated(self, val)
        if not isinstance(val, basestring):
            return as_local_id(self._prefix, val)
        return val


class TwitterDateTimeProperty(DateTimeProperty):
    def  __init__(self, name, **kwargs):
        format="%a %b %d %H:%M:%S +0000 %Y"
        DateTimeProperty.__init__(self, name, format, **kwargs)


class LocalUser(ModelPart):
    daily_tweets = FloatProperty('dt')
    rfriends_score = IntProperty('rfs')
    mention_score = IntProperty('ats')
    local_prob = EnumProperty('prob',[0,.5,1])


class User(TwitterModel):
    #contributors_enabled, follow_request_sent, following,
    #profile_background_color, profile_background_image_url,
    #profile_background_tile, , profile_link_color,
    #profile_sidebar_border_color, profile_sidebar_fill_color,
    #profile_text_color, profile_use_background_image,
    #show_all_inline_media, time_zone, status, notifications,

    _id = TwitterIdProperty('_id','U')
    local = ModelProperty('l',LocalUser)
    
    #properties from twitter
    verified = BoolProperty("ver")
    created_at = TwitterDateTimeProperty('ca')
    description = TextProperty('descr')
    favourites_count = IntProperty('favc')
    followers_count = IntProperty('folc')
    friends_count = IntProperty('frdc')
    geo_enabled = BoolProperty('geo')
    lang = TextProperty('lang')
    listed_count = IntProperty('lsc')
    location = TextProperty('loc')
    name = TextProperty('name')
    profile_image_url = TextProperty('img')
    protected = BoolProperty('prot')
    screen_name = TextProperty('sn')
    statuses_count = IntProperty('stc')
    url = TextProperty('url')
    utc_offset = IntProperty('utco')


class Tweet(TwitterModel):
    #contributors, entities, in_reply_to_screen_name, source,
    #truncated, user, id, retweeted, retweeted_status,
    #retweeted_count

    def __init__(self, from_dict=None, **kwargs):
        TwitterModel.__init__(self, from_dict, **kwargs)
        if self.mentions is None and 'entities' in from_dict:
            self.mentions = [
                as_local_id('U', at['id'])
                for at in from_dict['entities']['user_mentions']
            ]

    _id = TwitterIdProperty('_id','T')
    mentions = SlugListProperty('ats') #based on entities

    #properties from twitter
    coordinates = Property('coord')
    created_at = TwitterDateTimeProperty('ca')
    favorited = BoolProperty('fav')
    geo = Property('geo')
    in_reply_to_status_id = TwitterIdProperty('rtt','T')
    in_reply_to_user_id = TwitterIdProperty('rtu','U')
    place = Property('plc')
    text = TextProperty('tx')
    user_id = TwitterIdProperty('uid','U')

class Relationships(TwitterModel):
    # I only stored the first 5000 friends and followers
    _id = TwitterIdProperty('_id','R')
    friends = SlugListProperty('frs')
    followers = SlugListProperty('fols')
    
    @classmethod
    def get_for_user_id(cls, _id):
        _id[0] = 'R'
        return cls.get_id(_id)

    def rfriends(self):
        #figure out whether the user has more friends or followers
        lil,big = sorted([self.friends,self.followers],key=len)
        big = set(big)
        return [u for u in lil if u in big]

class JobBody(ModelPart):
    _id = TwitterIdProperty('_id','U')
    rfriends_score = IntProperty('rfs')
    mention_score = IntProperty('ats')
