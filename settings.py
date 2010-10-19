class SettingsBunch(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

settings = SettingsBunch(
    couchdb = 'http://127.0.0.1:5984/local',
)

try:
    from settings_prod import settings as s
except:
    from settings_dev import settings as s
settings.update(s)
