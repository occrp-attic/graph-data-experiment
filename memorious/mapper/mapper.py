import re
import six
from hashlib import sha1
from pybars import Compiler

from memorious.mapper.schema import Schema
from memorious.mapper.types import NameProperty, CountryProperty
from memorious.mapper.util import dict_list


class MapperProperty(object):
    FORMAT_PATTERN = re.compile('{{([^(}})]*)}}')
    compiler = Compiler()

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
            for ref in self.FORMAT_PATTERN.findall(self.format):
                self.refs.append(ref)
            # compile the format string into a handlebars template
            self.template = self.compiler.compile(six.text_type(self.format))

    def get_values(self, record):
        if self.literal is not None:
            return [self.literal]
        if self.format is not None:
            return [self.template(record)]
        return [record.get(r) for r in self.refs]

    def get_value(self, record):
        # select the first non-null value by default
        for value in self.get_values(record):
            if value is not None:
                return value


class Mapper(object):

    def __init__(self, dataset, data):
        self.dataset = dataset
        self.data = data
        self.keys = dict_list(data, 'keys', 'key')

        model = dataset.model
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
        return {p.name: p.get_value(record) for p in self.properties}

    def compute_key(self, record):
        if not len(self.keys):
            return None
        digest = sha1(self.dataset.name.encode('utf-8'))
        # digest.update(self.schema.name.encode('utf-8'))
        has_value = False
        for key in self.keys:
            value = record.get(key)
            if value is not None:
                value = unicode(value).encode('utf-8')
                digest.update(value)
                has_value = True
        if has_value:
            return digest.hexdigest()

    def to_index(self, record):
        # TODO make sure record and properties is typecast to strings
        return {
            'schema': self.schema.name,
            'schemata': list(self.schema.schemata),
            'dataset': self.dataset.name,
            'groups': self.dataset.groups,
            'properties': self.compute_properties(record),
            'fingerprints': [],
            'countries': [],
            'phones': [],
            'addresses': [],
            'text': record.text
        }


class EntityMapper(Mapper):
    section = Schema.ENTITY

    def __init__(self, dataset, name, data):
        self.name = name
        super(EntityMapper, self).__init__(dataset, data)

        if not len(self.keys):
            raise TypeError("No key column(s) defined: %s" % name)

    def to_index(self, record):
        data = super(EntityMapper, self).to_index(record)
        values = data['properties']
        data['id'] = self.compute_key(record)

        for prop in self.properties:
            value = values.get(prop.name)

            # Find an set the name property
            if prop.schema.is_label:
                data['name'] = value

            # Add fingerprinted properties.
            if isinstance(prop.type, NameProperty):
                fps = prop.type.normalize(value, record)
                data['fingerprints'].extend(fps)

            # Country codes index.
            if isinstance(prop.type, CountryProperty):
                ccs = prop.type.normalize(value, record)
                data['countries'].extend(ccs)

        # from pprint import pprint
        # pprint(data)
        return data


class LinkMapper(Mapper):
    section = Schema.LINK

    def __init__(self, dataset, data):
        super(LinkMapper, self).__init__(dataset, data)

    def to_index(self, record, entities):
        data = super(LinkMapper, self).to_index(record)

        # Add entity references for start and end
        data['entities'] = []
        for part in ['source', 'target']:
            name = self.data.get(part)
            if name not in entities or entities[name] is None:
                return None
            entity = entities[name]
            data[part] = {
                'id': entity.get('id'),
                'name': entity.get('name'),
                'fingerprints': entity.get('fingerprints')
            }
            data['fingerprints'].extend(entity.get('fingerprints'))
            data['entities'].append(entity.get('id'))

        # Generate a link ID
        digest = sha1()
        key_digest = self.compute_key(record)
        if key_digest is not None:
            digest.update(key_digest)
        for entity in data['entities']:
            digest.update(entity)
        data['id'] = digest.hexdigest()
        return data
