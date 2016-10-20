from pprint import pprint  # noqa

from memorious.core import es, es_index
from memorious.mapper.schema import Schema
from memorious.index.results import ResultSet, EntityResult

FACET_SIZE = 500
FILTERS = ['schemata', 'dataset', 'countries']


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

    must_filters = [{'terms': {'groups': auth.groups}}]
    for field_filter in FILTERS:
        values = query.getlist(field_filter)
        if values:
            must_filters.append({'terms': {field_filter: values}})

    aggregations = {}
    for facet in query.facets:
        aggregations.update({facet.field: {
            'terms': {'field': facet.field, 'size': FACET_SIZE}}
        })

    q = {
        'sort': ['_score'],
        'query': {
            'filtered': {
                'query': q,
                'filter': {
                    'bool': {
                        'must': must_filters
                    }
                }
            }
        },
        'size': query.limit,
        'from': query.offset,
        'aggregations': aggregations
    }

    # import json
    # print json.dumps(q)
    result = es.search(index=es_index, doc_type=Schema.ENTITY, body=q)
    return ResultSet(query, result)


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
