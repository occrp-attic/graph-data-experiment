import time
import logging
from elasticsearch import ElasticsearchException

from memorious.core import celery, model
from memorious.index import index_items
from memorious.model import Schema

log = logging.getLogger(__name__)
QUEUE_PAGE = 1000


def map_record(query, record):
    """Use the mapper to generate entities and links from a source row."""
    entities = {}
    for entity in query.entities:
        data = entity.to_index(record)
        if data is not None:
            entities[entity.name] = data
            yield (Schema.ENTITY, data['id'], data)

    for link in query.links:
        for inverted in [True, False]:
            data = link.to_index(record, entities, inverted=inverted)
            if data is not None:
                yield (Schema.LINK, data['id'], data)


@celery.task(bind=True)
def load_records(task, dataset_name, query_idx, records):
    """Load a single batch of QUEUE_PAGE records from the given query."""
    dataset = model.get_dataset(dataset_name)
    items = []
    for record in records:
        for item in map_record(dataset.queries[query_idx], record):
            items.append(item)

    try:
        index_items(items)
    except ElasticsearchException as exc:
        time.sleep(30)
        raise task.retry(exc=exc, countdown=30, max_retries=5)

    log.info("[%r] Indexed %s records as %s documents...",
             dataset_name, len(records), len(items))


def load_dataset(dataset):
    for query_idx, query in enumerate(dataset.queries):
        records = []
        for record_idx, record in enumerate(query.iterrecords()):
            records.append(record)
            if len(records) >= QUEUE_PAGE:
                load_records.delay(dataset.name, query_idx, records)
                records = []
            if record_idx != 0 and record_idx % 10000 == 0:
                log.info("Tasked %s records...", record_idx)
        if len(records):
            load_records.delay(dataset.name, query_idx, records)
