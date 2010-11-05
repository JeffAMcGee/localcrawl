import pdb
class SettingsBunch(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

settings = SettingsBunch(
    #FIXME: set local_box and couchdb for hou
    local_box = dict(lat=(30.5,31),lng=(-96.5,-96)),
    region = "fiddle",
    gisgraphy_url = "http://services.gisgraphy.com",
    beanstalk_host = 'localhost',
    beanstalk_port = 11300,
    couchdb_root = 'http://localhost:5984/',
    mention_weight = .5,
    crawl_ratio = .1,
    force_cutoff = 8,
    brain_in = 'brain.in',
    brain_out = 'brain.out',
    log_dir = 'logs',
    lookup_ttr = 3600,
    crawl_ttr = 480,
    tweets_per_hour = .04, # 1 tweet/day is median
    tweets_per_crawl = 20,
    max_hours = 100,
    pdb = pdb.set_trace
)

try:
    from settings_prod import settings as s
except:
    from settings_dev import settings as s
settings.update(s)
