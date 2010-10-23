class SettingsBunch(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

settings = SettingsBunch(
    couchdb = 'http://127.0.0.1:5984/bcstx',
    beanstalk_host = 'localhost',
    beanstalk_port = 11300,
    mention_weight = .5,
    crawl_ratio = .1,
    brain_in = 'jeffamcgee.in',
    brain_out = 'brain.out',
    beanstalk_ttr = 30,#3600
)

try:
    from settings_prod import settings as s
except:
    from settings_dev import settings as s
settings.update(s)
