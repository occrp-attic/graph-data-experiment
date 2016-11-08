from pprint import pprint  # noqa

from memorious.core import es, es_index
from memorious.model import Schema
from memorious.util import latinize_text
from memorious.index.results import ResultSet, EntityResult

FACET_SIZE = 500
LINK_FILTERS = ['schemata', 'remote.countries']
ENTITY_FILTERS = ['schemata', 'dataset', 'countries', 'phones',
                  'addresses', 'dates']


def search_entities(query, auth):
    if query.has_text:
        q = search_text(query.text,
                        ['name^6', 'fingerprints^2', 'text', '_all'])
    else:
        q = {'match_all': {}}
    q = compose_query(q, query, auth, ENTITY_FILTERS)

    # import json
    # print json.dumps(q)
    result = es.search(index=es_index, doc_type=Schema.ENTITY, body=q)
    return ResultSet(query, result)


def search_duplicates(entity_id, fingerprints, query, auth):
    """Find all the entities with the same fingerprints."""
    q = {
        'bool': {
            'must': [
                {'terms': {'fingerprints': list(fingerprints)}}
            ],
            'must_not': [
                {'terms': {'_id': [entity_id]}}
            ]
        }
    }
    q = compose_query(q, query, auth, ENTITY_FILTERS)
    result = es.search(index=es_index, doc_type=Schema.ENTITY, body=q)
    return ResultSet(query, result)


def search_links(entity, query, auth):
    q = {'term': {'origin.id': entity.id}}
    if query.has_text:
        fields = ['remote.name^6', 'remote.fingerprints^2', 'text', '_all']
        q = {
            'bool': {
                'must': [q, search_text(query.text, fields)]
            }
        }

    q = compose_query(q, query, auth, LINK_FILTERS)
    result = es.search(index=es_index, doc_type=Schema.LINK, body=q)
    return ResultSet(query, result, parent=entity)


def load_entity(entity_id, auth):
    # TODO: where to put in authz?
    q = {
        'query': {
            'bool': {
                'must': [
                    {'term': {'_id': entity_id}},
                    {'terms': {'groups': auth.groups}}
                ]
            }
        }
    }
    result = es.search(index=es_index, doc_type=Schema.ENTITY, body=q)
    hits = result.get('hits', {})
    if hits.get('total') != 1:
        return None
    for document in hits.get('hits', []):
        return EntityResult(document)


def search_text(text, fields):
    q = {
        'query_string': {
            'query': text,
            'fields': fields,
            'default_operator': 'AND'
        }
    }
    latin_text = latinize_text(text)
    if latin_text != text:
        latin_q = {
            'query_string': {
                'query': latin_text,
                'fields': fields,
                'default_operator': 'AND'
            }
        }
        q = {
            'bool': {
                'should': [q, latin_q],
                'minimum_should_match': 1
            }
        }
    return q


def compose_query(q, query, auth, filters):
    return {
        'sort': ['_score'],
        'query': {
            'filtered': {
                'query': q,
                'filter': {
                    'bool': {
                        'must': filter_query(query, auth, filters)
                    }
                }
            }
        },
        'size': query.limit,
        'from': query.offset,
        'aggregations': aggregate_query(query)
    }


def filter_query(query, auth, filters):
    """Apply filters specified in the query."""
    must_filters = [{'terms': {'groups': auth.groups}}]
    for field_filter in filters:
        values = query.getlist(field_filter)
        if values:
            must_filters.append({'terms': {field_filter: values}})
    return must_filters


def aggregate_query(query):
    """Generate aggregations from the query's facets."""
    aggregations = {}
    for facet in query.facets:
        aggregations.update({facet.field: {
            'terms': {'field': facet.field, 'size': FACET_SIZE}}
        })
    return aggregations
