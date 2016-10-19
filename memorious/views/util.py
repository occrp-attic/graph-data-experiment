from memorious.core import model
from memorious.mapper.schema import Schema


def dataset_label(key):
    for dataset in model.datasets:
        if dataset.name == key:
            return dataset.label
    return key


def entity_schema_label(key):
    try:
        schema = model.get_schema(Schema.ENTITY, key)
        return schema.label
    except Exception:
        return key
