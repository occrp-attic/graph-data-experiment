import os
import logging
import urllib3

# loggers.
logging.basicConfig(level=logging.DEBUG)

# urllib3
urllib3.disable_warnings()
logging.getLogger('urllib3').setLevel(logging.WARNING)

# specific loggers
logging.getLogger('pyelasticsearch').setLevel(logging.WARNING)
logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logging.getLogger('assets.cssutils').setLevel(logging.ERROR)
logging.getLogger('cssutils').setLevel(logging.ERROR)
logging.getLogger('amqp').setLevel(logging.INFO)

# Log all SQL statements:
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# default locale settings
os.environ['LC_ALL'] = 'en_US'
os.environ['LC_LANG'] = 'en_US'
os.environ['LC_CTYPE'] = 'en_US'
os.environ['LANG'] = 'en_US'

# Python requests
# using SSL w/o certificate validation

# requests.packages.urllib3.disable_warnings()
# logging.getLogger('requests').setLevel(logging.WARNING)

# logging.getLogger('httpstream').setLevel(logging.WARNING)
