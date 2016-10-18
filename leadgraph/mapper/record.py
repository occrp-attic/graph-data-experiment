from leadgraph.mapper.schema import Schema


class Record(dict):

    def __init__(self, source, row):
        self.source = source
        self.update(row)

    def iterindex(self, record):
        entities = {}
        for entity in self.source.entities:
            entities[entity.name] = entity.to_index(record)
            if entities[entity.name] is not None:
                yield (Schema.ENTITY, entities[entity.name])

        for link in self.source.links:
            data = link.to_index(record, entities)
            if data is not None:
                yield (Schema.LINK, data)

    def __repr__(self):
        return '<Record(%r)>' % (self.source)
