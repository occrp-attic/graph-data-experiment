import logging
from pprint import pprint  # noqa
from hashlib import sha1
from elasticsearch.helpers import bulk, scan

from memorious.core import es, es_index
from memorious.model import Schema
from memorious.util import chunk_iter

log = logging.getLogger(__name__)
CHUNK_SIZE = 5000
CROSSREF_TYPE = 'crossref'


def _scan_fingerprints(dataset_name=None):
    if dataset_name:
        q = {'term': {'dataset': dataset_name}}
    else:
        q = {'match_all': {}}
    q = {
        'query': q,
        '_source': ['fingerprints', 'dataset']
    }

    scan_iter = scan(es, query=q, index=es_index, doc_type=Schema.ENTITY)
    for i, doc in enumerate(scan_iter):
        source = doc.get('_source')
        for fp in source.get('fingerprints'):
            yield fp, source.get('dataset')
        if i != 0 and i % 10000 == 0:
            log.info("Crossref: %s entities...", i)


def _update_entities(dataset_name=None):
    fps = _scan_fingerprints(dataset_name=dataset_name)
    for chunk in chunk_iter(fps, CHUNK_SIZE):
        queries = []
        for (fp, dataset) in chunk:
            doc_id = sha1(fp).hexdigest()
            queries.append({
                '_id': doc_id,
                '_type': CROSSREF_TYPE
            })

        result = es.mget(index=es_index, body={'docs': queries})
        for item, doc in zip(chunk, result.get('docs')):
            fp, dataset = item
            datasets = set([dataset])
            if doc.get('found', False):
                old = doc['_source']['datasets']
                if dataset in old:
                    continue
                datasets.update(old)
            yield {
                '_id': doc['_id'],
                '_type': CROSSREF_TYPE,
                '_index': str(es_index),
                '_source': {
                    'fingerprint': fp,
                    'datasets': list(datasets),
                    'length': len(datasets)
                }
            }


def generate_crossref(dataset_name=None):
    bulk(es, _update_entities(dataset_name=dataset_name), stats_only=True,
         chunk_size=CHUNK_SIZE, request_timeout=200.0)
