import beanstalkc
c = beanstalkc.Connection()
c.use('lookup')

def kick():
    c.put('{"_id":"U106582358"}')
    #c.put('{"_id":"U17560063"}')

def clear(tube):
    c.watch(tube)
    while True:
        j = c.reserve(0)
        if j is None:
            return
        print j.body
        j.delete()

