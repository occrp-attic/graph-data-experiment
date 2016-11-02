import logging
from elasticsearch.helpers import bulk, scan

from memorious.core import es, es_index
from memorious.schema import Schema
from memorious.util import DATA_PAGE, chunk_iter, is_list, remove_nulls

log = logging.getLogger(__name__)
INDEX_PAGE = 1000


def _dataset_iter(dataset):
    row_idx = 1
    # TODO: make an LRU cache on (document IDs, doc_type)
    for query in dataset.queries:
        for record in query.iterrecords():
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


def merge_doc_objects(old, new):
    """Exend the values of the new doc with extra values from the old."""
    old = remove_nulls(old)
    new = remove_nulls(new)
    for k, v in old.items():
        if k in new:
            if is_list(v):
                v = new[k] + v
                new[k] = list(set(v))
            elif isinstance(v, dict):
                new[k] = merge_doc_objects(v, new[k])
        else:
            new[k] = v
    return new


def _index_updates(dataset):
    """Look up existing index documents and generate an updated form.

    This is necessary to make the index accumulative, i.e. if an entity or link
    gets indexed twice with different field values, it'll add up the different
    field values into a single record. This is to avoid overwriting the
    document and losing field values. An alternative solution would be to
    implement this in Groovy on the ES.
    """
    for chunk in chunk_iter(_dataset_iter(dataset), INDEX_PAGE):
        query_docs, new_docs = [], []
        for doc in chunk:
            # Slight hack: no need to upsert links, the problem (AFAIK) only
            # exists for entities.
            if doc.get('_type') == Schema.LINK:
                yield doc
            else:
                query_docs.append({
                    '_id': doc['_id'],
                    '_type': doc['_type']
                })
                new_docs.append(doc)
        result = es.mget(index=es_index, body={'docs': query_docs})
        for new_doc, idx_doc in zip(new_docs, result.get('docs')):
            assert new_doc['_id'] == idx_doc['_id']
            idx_doc.pop('_version', None)
            if idx_doc.pop('found', False):
                new_doc = merge_doc_objects(idx_doc, new_doc)
            yield new_doc


def index_dataset(dataset):
    bulk(es, _index_updates(dataset), stats_only=True,
         chunk_size=INDEX_PAGE, request_timeout=200.0)
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
