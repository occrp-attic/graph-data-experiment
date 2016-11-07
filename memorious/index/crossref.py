from pprint import pprint  # noqa

from memorious.core import es, es_index
from memorious.model import Schema


def expand_fingerprints(fingerprints, auth, depth=0):
    """Given a set of fingerprints, find more related FPs via entities."""
    fingerprints = set(fingerprints)
    while True:
        q = {
            'size': 0,
            'query': {
                'bool': {
                    'must': [
                        {'terms': {'fingerprints': list(fingerprints)}},
                        {'terms': {'groups': auth.groups}}
                    ]
                }
            },
            'aggregations': {
                'fingerprints': {
                    'terms': {'field': 'fingerprints', 'size': 1000}
                }
            }
        }
        result = es.search(index=es_index, doc_type=Schema.ENTITY, body=q)
        buckets = result['aggregations']['fingerprints']['buckets']
        old_fingerprints = len(fingerprints)
        for bucket in buckets:
            fingerprints.add(bucket['key'])
        if len(fingerprints) == old_fingerprints:
            return fingerprints
