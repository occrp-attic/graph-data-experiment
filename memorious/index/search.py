from pprint import pprint  # noqa

from memorious.core import es, es_index
from memorious.schema import Schema
from memorious.index.results import ResultSet, EntityResult

FACET_SIZE = 500
LINK_FILTERS = ['schemata']


def search_entities(query, auth):
    if query.has_text:
        q = {
            'query_string': {
                'query': query.text,
                'fields': ['name^6', 'fingerprints^2', 'text', '_all'],
                'default_operator': 'AND',
                'use_dis_max': True
            }
        }
    else:
        q = {'match_all': {}}
    q = compose_query(q, query, auth, ['schemata', 'dataset', 'countries',
                                       'phones', 'addresses'])

    # import json
    # print json.dumps(q)
    result = es.search(index=es_index, doc_type=Schema.ENTITY, body=q)
    return ResultSet(query, result)


def search_links(entity, query, auth):
    q = {'term': {'origin.id': entity.id}}
    if query.has_text:
        textq = {
            'query_string': {
                'query': query.text,
                'fields': ['remote.name^6', 'remote.fingerprints^2',
                           'text', '_all'],
                'default_operator': 'AND',
                'use_dis_max': True
            }
        }
        q = {
            'bool': {
                'must': [q, textq]
            }
        }

    q = compose_query(q, query, auth, ['schemata', 'remote.countries'])
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
