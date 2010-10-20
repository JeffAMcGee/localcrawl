import beanstalkc
c = beanstalkc.Connection()
c.use('users')
c.put('{"_id":"U106582358"}')
