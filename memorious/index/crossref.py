import logging
from pprint import pprint  # noqa
from hashlib import sha1
from elasticsearch.helpers import bulk, scan

from memorious.core import es, es_index, model
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
        fps = source.get('fingerprints')
        if fps is None:
            continue
        for fp in fps:
            if fp is None:
                continue
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


def search_datasets(datasets):
    if len(datasets):
        q = {
            'bool': {
                'must': [{'term': {'datasets': d}} for d in datasets]
            }
        }
    else:
        q = {'match_all': {}}
    q = {
        'size': 0,
        'query': q,
        'aggregations': {
            'datasets': {
                'terms': {'field': 'datasets', 'size': 1000}
            }
        }
    }
    result = es.search(index=es_index, doc_type=CROSSREF_TYPE, body=q)
    buckets = result['aggregations']['datasets']['buckets']
    counts = []
    for bucket in buckets:
        dataset = bucket.get('key')
        if dataset in datasets:
            continue
        try:
            dataset = model.get_dataset(dataset)
        except TypeError as te:
            log.exception(te)
        counts.append((dataset, bucket.get('doc_count')))
    return counts


def search_crossref_entities(datasets, query):
    q = {
        'bool': {
            'must': [{'term': {'datasets': d}} for d in datasets]
        }
    }
    q = {
        'size': query.limit,
        'from': query.offset,
        'query': q,
        '_source': ['fingerprint']
    }
    result = es.search(index=es_index, doc_type=CROSSREF_TYPE, body=q)
    return result
