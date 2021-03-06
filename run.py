#!/usr/bin/env python

if __name__ != '__main__':
    print """
This is a tool for testing and administrative tasks.  It is designed to
be %run in ipython or from the command line.  If you import it from another
module, you're doing something wrong.  
"""

import logging
import sys
import getopt

import beanstalkc
import maroon

from settings import settings
import twitter
from gisgraphy import GisgraphyResource
from maroon import *
from models import *
from admin import *
from peek import *
from utils import *

logging.basicConfig(level=logging.INFO)

if len(sys.argv)>1:
    try:
        opts, args = getopt.getopt(sys.argv[2:], "c:m:s:e:")
    except getopt.GetoptError, err:
        print str(err)
        print "usage: ./admin.py function_name [-c couchdb] [-m mongodb] [-s startkey] [-e endkey] [arguments]"
        sys.exit(2)
    kwargs={}
    for o, a in opts:
        if o == "-c":
            Model.database = couch(a)
        elif o == "-m":
            Model.database = mongo(a)
        elif o == "-s":
            kwargs['start']=a
        elif o == "-e":
            kwargs['end']=a
        else:
            assert False, "unhandled option"
    locals()[sys.argv[1]](*args,**kwargs)
else:
    gisgraphy = GisgraphyResource()
    twitter = twitter.TwitterResource()
    Model.database = mongo(settings.region)
    try:
        stalk = beanstalkc.Connection()
    except:
        pass
