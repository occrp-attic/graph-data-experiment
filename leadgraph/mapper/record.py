from leadgraph.mapper.schema import Schema


class Record(dict):

    def __init__(self, dataset, row):
        self.dataset = dataset
        self.update(row)

    def get_ref(self, ref):
        return self.get(ref)

    def iterindex(self, record):
        entities = {}
        for entity in self.dataset.entities:
            data = entity.to_index(record)
            if data is not None:
                entities[entity.name] = data
                yield (Schema.ENTITY, entities[entity.name])

        for link in self.dataset.links:
            data = link.to_index(record, entities)
            if data is not None:
                yield (Schema.LINK, data)

    def __repr__(self):
        return '<Record(%r)>' % (self.dataset)
