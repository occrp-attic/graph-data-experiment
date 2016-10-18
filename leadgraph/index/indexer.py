import logging
from elasticsearch.helpers import bulk

from leadgraph.core import es, es_index
from leadgraph.util import DATA_PAGE

log = logging.getLogger(__name__)


def _source_iter(source):
    row_idx = 1
    # TODO: make an LRU cache on (document IDs, doc_type)
    for record in source.iterrecords():
        for (doc_type, doc) in record.iterindex(record):
            doc = dict(doc.items())
            doc_id = doc.pop('id', None)
            yield {
                '_id': doc_id,
                '_type': doc_type,
                '_index': str(es_index),
                '_source': doc
            }

            if row_idx % 10 == 0:
                log.info("Index %r: %s", source, row_idx)
            row_idx += 1


def index_source(source):
    bulk(es, _source_iter(source), stats_only=True,
         chunk_size=DATA_PAGE, request_timeout=200.0)
