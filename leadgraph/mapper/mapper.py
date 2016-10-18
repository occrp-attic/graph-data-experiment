from leadgraph.mapper.schema import Schema
from leadgraph.mapper.util import dict_list


class MapperProperty(object):

    def __init__(self, mapper, name, data, schema):
        self.mapper = mapper
        self.name = name
        self.data = data
        self.schema = schema
        self.ref = data.get('column')

    def get_value(self, record):
        return record.get(self.ref)


class Mapper(object):

    def __init__(self, source, data):
        self.source = source
        self.data = data
        self.keys = dict_list(data, 'keys', 'key')

        self.schema = source.model.get_schema(self.section, data.get('schema'))
        if self.schema is None:
            raise TypeError("Invalid schema: %r" % data.get('schema'))

        self.properties = []
        for name, prop in data.get('properties', {}).items():
            schema = self.schema.get(name)
            self.properties.append(MapperProperty(self, name, prop, schema))

    def get_properties(self, record):
        return {p.name: p.get_value(record) for p in self.properties}

    def to_index(self, record):
        # TODO make sure record is typecast to strings
        return {
            'schema': self.schema.name,
            'source': self.source.name,
            'properties': self.get_properties(record),
            # 'record': dict(record)
        }


class EntityMapper(Mapper):
    section = Schema.ENTITY

    def __init__(self, source, name, data):
        self.name = name
        super(EntityMapper, self).__init__(source, data)

    def to_index(self, record):
        data = super(EntityMapper, self).to_index(record)
        data['id'] = 'foo'
        return data


class LinkMapper(Mapper):
    section = Schema.LINK

    def __init__(self, source, data):
        super(LinkMapper, self).__init__(source, data)
        self.source = data.get('source')
        self.target = data.get('target')

    def to_index(self, record, entities):
        data = super(LinkMapper, self).to_index(record)
        source = entities.get(self.source)
        target = entities.get(self.target)
        if source is None or target is None:
            return None
        return data
