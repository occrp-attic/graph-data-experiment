import os

DEBUG = True

APP_NAME = 'memorious'
APP_TITLE = 'Memorious'

DATABASE_URL = os.environ.get('DATABASE_URI')
ELASTICSEARCH_URL = 'http://localhost:9200/'
# ELASTICSEARCH_INDEX = APP_NAME
MODEL_YAML = 'model.yaml'
