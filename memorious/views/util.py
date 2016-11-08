from flask import request
from datetime import datetime
from urlparse import urlparse, urljoin

from memorious.core import model
from memorious.model import Schema
from memorious.reference import COUNTRY_NAMES


def dataset_label(key):
    for dataset in model.datasets:
        if dataset.name == key:
            return dataset.label
    return key


def entity_schema_label(key):
    try:
        schema = model.get_schema(Schema.ENTITY, key)
        return schema.label
    except Exception:
        return key


def entity_schema_icon(key):
    try:
        schema = model.get_schema(Schema.ENTITY, key)
        return schema.icon
    except Exception:
        return key


def link_schema_label(key):
    try:
        schema = model.get_schema(Schema.LINK, key)
        return schema.label
    except Exception:
        return key


def country(key):
    return COUNTRY_NAMES.get(key, key)


def date(value):
    try:
        dt = datetime.strptime(value, '%Y-%m-%d')
        return dt.strftime('%d.%m.%Y')
    except ValueError:
        return value


def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and \
        ref_url.netloc == test_url.netloc
