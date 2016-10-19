import logging
from elasticsearch.helpers import bulk

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
         chunk_size=DATA_PAGE, request_timeout=200.0)
