import logging
from flask import Flask, current_app
from flask import url_for as flask_url_for
from flask_mail import Mail
from flask_assets import Environment
from flask_oauthlib.client import OAuth
from werkzeug.local import LocalProxy
from sqlalchemy import create_engine, MetaData
from celery import Celery
from elasticsearch import Elasticsearch

from memorious import default_settings
from memorious.util import load_config_file

log = logging.getLogger(__name__)

mail = Mail()
assets = Environment()
oauth = OAuth()
oauth_provider = oauth.remote_app('provider', app_key='OAUTH')
celery = Celery('memorious')


def create_app(config={}):
    app = Flask('memorious')
    app.config.from_object(default_settings)
    app.config.from_envvar('MEMORIOUS_SETTINGS', silent=True)
    app.config.update(config)
    mail.init_app(app)
    assets.init_app(app)
    oauth.init_app(app)
    celery.conf.update(app.config)
    return app


def get_config(name, default=None):
    return current_app.config.get(name, default)


def url_for(*a, **kw):
    """Generate external URLs with HTTPS (if configured)."""
    try:
        kw['_external'] = True
        if get_config('PREFERRED_URL_SCHEME'):
            kw['_scheme'] = get_config('PREFERRED_URL_SCHEME')
        return flask_url_for(*a, **kw)
    except RuntimeError:
        return None


def get_app_name():
    return get_config('APP_NAME', 'aleph')


def get_app_title():
    return get_config('APP_TITLE', get_app_name())


def get_engine():
    app = current_app._get_current_object()
    if not hasattr(app, '_sa_engine'):
        app._sa_engine = create_engine(get_config('DATABASE_URL'))
    return app._sa_engine


def get_metadata():
    app = current_app._get_current_object()
    if not hasattr(app, '_sa_meta'):
        app._sa_meta = MetaData()
        app._sa_meta.bind = get_engine()
    return app._sa_meta


def get_model():
    app = current_app._get_current_object()
    if not hasattr(app, '_lg_model'):
        from memorious.model import Model
        model_config = get_config('MODEL_YAML', 'model.yaml')
        app._lg_model = Model(load_config_file(model_config))
    return app._lg_model


def get_es():
    app = current_app._get_current_object()
    if not hasattr(app, '_es_instance'):
        app._es_instance = Elasticsearch(get_config('ELASTICSEARCH_URL'),
                                         timeout=240, sniff_on_start=True)
    return app._es_instance


def get_es_index():
    return get_config('ELASTICSEARCH_INDEX', get_app_name())


app_name = LocalProxy(get_app_name)
app_title = LocalProxy(get_app_title)
engine = LocalProxy(get_engine)
meta = LocalProxy(get_metadata)
model = LocalProxy(get_model)
es = LocalProxy(get_es)
es_index = LocalProxy(get_es_index)
