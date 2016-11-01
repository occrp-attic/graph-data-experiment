from flask import request
from urlparse import urlparse, urljoin
from pycountry import countries

from memorious.core import model
from memorious.schema import Schema

COUNTRY_NAMES = {
    'ZZ': 'Global',
    'EU': 'European Union',
    'XK': 'Kosovo'
}

for country in countries:
    COUNTRY_NAMES[country.alpha2] = country.name


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


def link_schema_label(key):
    try:
        schema = model.get_schema(Schema.LINK, key)
        return schema.label
    except Exception:
        return key


def country_label(key):
    return COUNTRY_NAMES.get(key, key)


def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and \
        ref_url.netloc == test_url.netloc
