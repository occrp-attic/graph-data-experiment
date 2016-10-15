import logging
from flask import Flask, current_app
from flask import url_for as flask_url_for
from flask_mail import Mail
from elasticsearch import Elasticsearch

from leadgraph import default_settings

log = logging.getLogger(__name__)

mail = Mail()


def create_app(config={}):
    app = Flask('leadgraph')
    app.config.from_object(default_settings)
    app.config.from_envvar('LEADGRAPH_SETTINGS', silent=True)
    app.config.update(config)
    mail.init_app(app)
    return app


def get_config(name, default=None):
    return current_app.config.get(name, default)


def get_app_name():
    return current_app.config.get('APP_NAME', 'aleph')


def get_app_title():
    return current_app.config.get('APP_TITLE') or get_app_name()


def get_es():
    app = current_app._get_current_object()
    if not hasattr(app, '_es_instance'):
        app._es_instance = Elasticsearch(app.config.get('ELASTICSEARCH_URL'),
                                         timeout=120)
    return app._es_instance


def get_es_index():
    app = current_app._get_current_object()
    return app.config.get('ELASTICSEARCH_INDEX', get_app_name())


def url_for(*a, **kw):
    """Generate external URLs with HTTPS (if configured)."""
    try:
        kw['_external'] = True
        if get_config('PREFERRED_URL_SCHEME'):
            kw['_scheme'] = get_config('PREFERRED_URL_SCHEME')
        return flask_url_for(*a, **kw)
    except RuntimeError:
        return None
