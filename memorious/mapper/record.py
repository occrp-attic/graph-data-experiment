import six

from memorious.schema import Schema


class Record(dict):

    def __init__(self, query, row):
        self.query = query
        self.update(row)

        self.text = []
        for text in self.values():
            if isinstance(text, six.string_types) and len(text.strip()):
                self.text.append(text)

    def iterindex(self, record):
        entities = {}
        for entity in self.query.entities:
            data = entity.to_index(record)
            if data is not None:
                entities[entity.name] = data
                yield (Schema.ENTITY, entities[entity.name])

        for link in self.query.links:
            for inverted in [True, False]:
                data = link.to_index(record, entities, inverted=inverted)
                if data is not None:
                    yield (Schema.LINK, data)

    def __repr__(self):
        return '<Record(%r)>' % (self.query)
