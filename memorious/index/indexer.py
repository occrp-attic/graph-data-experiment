import logging
from elasticsearch.helpers import bulk, scan

from memorious.core import es, es_index
from memorious.util import DATA_PAGE

log = logging.getLogger(__name__)


def _dataset_iter(dataset):
    row_idx = 1
    # TODO: make an LRU cache on (document IDs, doc_type)
    for record in dataset.iterrecords():
        for (doc_type, doc) in record.iterindex(record):
            doc = dict(doc.items())
            doc_id = doc.pop('id', None)
            if doc_id is None:
                continue

            # from pprint import pprint
            # pprint(doc)

            yield {
                '_id': doc_id,
                '_type': doc_type,
                '_index': str(es_index),
                '_source': doc
            }

            if row_idx % 10000 == 0:
                log.info("Index %r: %s", dataset, row_idx)
            row_idx += 1


def index_dataset(dataset):
    bulk(es, _dataset_iter(dataset), stats_only=True,
         chunk_size=DATA_PAGE / 10.0, request_timeout=200.0)
    optimize_search()


def delete_dataset(dataset):
    """Delete all entries from a particular dataset."""
    q = {'query': {'term': {'dataset': dataset.name}}, '_source': False}

    def deletes():
        for i, res in enumerate(scan(es, query=q, index=es_index)):
            yield {
                '_op_type': 'delete',
                '_index': str(es_index),
                '_type': res.get('_type'),
                '_id': res.get('_id')
            }
            if i > 0 and i % 10000 == 0:
                log.info("Delete %s: %s", dataset, i)
    es.indices.refresh(index=es_index)
    bulk(es, deletes(), stats_only=True, chunk_size=DATA_PAGE)
    optimize_search()


def optimize_search():
    """Run a full index restructure. May take a while."""
    es.indices.optimize(index=es_index)
