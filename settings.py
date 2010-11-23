import pdb
class SettingsBunch(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

settings = SettingsBunch(
    local_box = dict(lat=(29,30.5),lng=(-96,-94.5)),
    region = "houtx",
    slaves = 12,
    gisgraphy_url = "http://services.gisgraphy.com",
    beanstalk_host = 'localhost',
    beanstalk_port = 11300,
    couchdb_root = 'http://localhost:5984/',
    mention_weight = .5,
    crawl_ratio = .1,
    force_cutoff = 8,
    min_cutoff = 2,
    lookup_in = 'lookup.in',
    lookup_out = 'lookup.out',
    log_dir = 'logs',
    beanstalkd_ttr = 3600,
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
