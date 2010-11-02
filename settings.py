class SettingsBunch(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

settings = SettingsBunch(
    local_box = dict(lat=(30.5,31),lng=(-96.5,-96)),
    couchdb = 'http://127.0.0.1:5984/bcstx',
    gisgraphy_url = "http://services.gisgraphy.com",
    beanstalk_host = 'localhost',
    beanstalk_port = 11300,
    mention_weight = .5,
    crawl_ratio = .1,
    force_cutoff = 8,
    brain_in = 'brain.in',
    brain_out = 'brain.out',
    beanstalk_ttr = 3600,
    tweets_per_hour = .04 # 1 tweet/day is median
)

try:
    from settings_prod import settings as s
except:
    from settings_dev import settings as s
settings.update(s)
