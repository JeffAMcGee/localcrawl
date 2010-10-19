import datetime
import maroon
from maroon import *


def as_local_id(prefix,id):
    return "%c%d"%(prefix,id)


def as_int_id(id):
    return int(id[1:])


class TwitterIdProperty(TextProperty):
    def __init__(self, name, prefix, **kwargs):
        TextProperty.__init__(self, name, **kwargs)
        self._prefix = prefix

    def validated(self, val):
        val = Property.validated(self, val)
        if not isinstance(val, basestring):
            return as_local_id(self._prefix, val)
        return val
 
    #FIXME: deal with local and int ids


class TwitterDateTimeProperty(DateTimeProperty):
    def  __init__(self, name, **kwargs):
        format="%a %b %d %H:%M:%S +0000 %Y"
        DateTimeProperty.__init__(self, name, format, **kwargs)


class LocalUser(ModelPart):
    daily_tweets = FloatProperty('dt')
    depth = IntProperty('d')
    rfriends = IntProperty('rf')
    mentions = IntProperty('at')
    state = EnumProperty('s',['new','look','crawl','ignore'])
    local_prob = EnumProperty('prob',[0,.5,1])


class User(Model):
    ignore = [
        'contributors_enabled', 'follow_request_sent', 'following',
        'profile_background_color', 'profile_background_image_url',
        'profile_background_tile', '', 'profile_link_color',
        'profile_sidebar_border_color', 'profile_sidebar_fill_color',
        'profile_text_color', 'profile_use_background_image',
        'show_all_inline_media', 'time_zone', 'status', 'notifications',
    ]

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


class Tweet(Model):
    ignore = [
        'contributors', 'entities', 'in_reply_to_screen_name', 'source',
        'truncated', 'user', 'id', 'retweeted', 'retweeted_status',
        'retweeted_count'
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
    retweeted = Property('rt') #do we want this?
    retweeted_count = IntProperty('rtc')
    text = TextProperty('tx')
    user_id = TwitterIdProperty('uid','U')
