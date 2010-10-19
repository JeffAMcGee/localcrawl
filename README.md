  
localcrawl
========    

The lo-calorie twitter neighborhood crawler.


LICENSE
-------

This falls under the same license as TornadoWeb,
the Apache License, Version 2.0
Get a copy at http://www.apache.org/licenses/LICENSE-2.0.html

GETTING STARTED
---------------

You'll need to create a `settings_dev.py` or `settings_prod.py` in order to run
localcrawl.  The easiest way to do this is to copy settings_dev.py.template to
settings_dev.py.

    cp settings_dev.py.template settings_dev.py

You will also need to go through the three-legged oauth to get the information
that goes in settings_dev.py .  This will help you:
    http://benoitc.github.com/restkit/authentication.html#oauth
