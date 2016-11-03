import six
from hashlib import sha1
from pprint import pprint  # noqa

from memorious.schema import Schema
from memorious.mapper.formatting import Formatter
from memorious.util import dict_list


class MapperProperty(object):

    def __init__(self, mapper, name, data, schema):
        self.mapper = mapper
        self.name = name
        self.data = data
        self.schema = schema
        self.type = schema.type_cls(self)
        self.refs = dict_list(data, 'column', 'columns')
        self.literal = data.get('literal')

        # this is hacky, trying to generate refs from template
        self.format = data.get('format')
        if self.format is not None:
            self.formatter = Formatter(self.format)
            self.refs.extend(self.formatter.refs)

    def get_values(self, record):
        values = []
        if self.format is not None:
            values.append(self.formatter.apply(record))
        else:
            for r in self.refs:
                values.append(record.get(r))
        values.append(self.literal)
        values = [self.type.clean(v, record) for v in values]
        return list(set([v for v in values if v is not None]))

    def get_value(self, record):
        # Select the first non-null value by default (like SQL COALESCE())
        for value in self.get_values(record):
            return value


class Mapper(object):

    def __init__(self, query, data):
        self.query = query
        self.data = data
        self.keys = dict_list(data, 'keys', 'key')

        model = query.dataset.model
        self.schema = model.get_schema(self.section, data.get('schema'))
        if self.schema is None:
            raise TypeError("Invalid schema: %r" % data.get('schema'))

        self.properties = []
        for name, prop in data.get('properties', {}).items():
            schema = self.schema.get(name)
            self.properties.append(MapperProperty(self, name, prop, schema))

    @property
    def refs(self):
        for key in self.keys:
            yield key
        for prop in self.properties:
            for ref in prop.refs:
                yield ref

    def compute_properties(self, record):
        return {p.name: p.get_values(record) for p in self.properties}

    def compute_key(self, record):
        if not len(self.keys):
            return None
        digest = sha1(self.query.dataset.name.encode('utf-8'))
        # digest.update(self.schema.name.encode('utf-8'))
        for key in self.keys:
            value = record.get(key)
            if value is None:
                return
            value = six.text_type(value)
            if not len(value.strip()):
                return
            digest.update(value.encode('utf-8'))
        return digest.hexdigest()

    def to_index(self, record):
        # TODO make sure record and properties is typecast to strings
        return {
            'schema': self.schema.name,
            'schemata': list(self.schema.schemata),
            'dataset': self.query.dataset.name,
            'groups': self.query.dataset.groups,
            'properties': self.compute_properties(record),
            'text': record.text
        }


class EntityMapper(Mapper):
    section = Schema.ENTITY

    def __init__(self, query, name, data):
        self.name = name
        super(EntityMapper, self).__init__(query, data)

        if not len(self.keys):
            raise TypeError("No key column(s) defined: %s" % name)

    def to_index(self, record):
        data = super(EntityMapper, self).to_index(record)
        properties = data['properties']
        data['id'] = self.compute_key(record)
        if not data['id']:
            return

        for prop in self.properties:
            values = properties.get(prop.name)

            # Find an set the name property
            if prop.schema.is_label and len(values):
                data['name'] = values[0]

            # Add inverted properties. This takes all the properties
            # of a specific type (names, dates, emails etc.)
            if prop.type.index_invert:
                if prop.type.index_invert not in data:
                    data[prop.type.index_invert] = []
                norm = prop.type.normalize(values, record)
                data[prop.type.index_invert].extend(norm)

        # pprint(data)
        return data


class LinkMapper(Mapper):
    section = Schema.LINK

    def __init__(self, query, data):
        super(LinkMapper, self).__init__(query, data)

    def to_index(self, record, entities, inverted=False):
        data = super(LinkMapper, self).to_index(record)
        data['inverted'] = inverted

        source, target = self.data.get('source'), self.data.get('target')
        if inverted:
            origin, remote = entities.get(target), entities.get(source)
        else:
            origin, remote = entities.get(source), entities.get(target)

        if origin is None or remote is None:
            # If data was missing for either the source or target entity
            # they will be None, and we should not create a link.
            return

        # We don't need to index the entity here, since it's already known
        # in the simplest case (entity profile pages).
        data['origin'] = {
            'id': origin.get('id'),
            'fingerprints': origin.get('fingerprints'),
        }
        data['remote'] = remote

        # Generate a link ID
        digest = sha1()
        digest.update(str(inverted))
        digest.update(origin['id'])
        digest.update(remote['id'])
        key_digest = self.compute_key(record)
        if key_digest is not None:
            digest.update(key_digest)
        data['id'] = digest.hexdigest()
        return data
