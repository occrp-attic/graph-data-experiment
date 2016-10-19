from pprint import pprint  # noqa

from memorious.core import es, es_index
from memorious.mapper.schema import Schema
from memorious.index.results import ResultSet


def search_entities(query):
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

    q = {
        'sort': ['_score'],
        'query': {
            'filtered': {
                'query': q,
                'filter': {
                    'bool': {
                        'must': [],
                        'should': []
                    }
                }
            }
        },
        'size': query.limit,
        'from': query.offset,
        'aggregations': {}
    }

    import json
    print json.dumps(q)
    result = es.search(index=es_index, doc_type=Schema.ENTITY, body=q)
    return ResultSet(query, result)
