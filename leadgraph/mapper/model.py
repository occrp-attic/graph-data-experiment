from leadgraph.mapper.schema import Schema
from leadgraph.mapper.source import Source


class Model(object):

    def __init__(self, data):
        self.data = data

        self.schemata = []
        for section in Schema.SECTIONS:
            for name, sconfig in data['schema'].get(section, {}).items():
                self.schemata.append(Schema(section, name, sconfig))

        self.sources = []
        for name, sconfig in data.get('sources', {}).items():
            self.sources.append(Source(self, name, sconfig))

    def get_schema(self, section, name):
        for schema in self.schemata:
            if schema.section == section and schema.name == name:
                return schema
        raise TypeError("No such schema for %s: %s" % (section, name))

    def __repr__(self):
        return '<Model(%r, %r)>' % (self.schemata, self.sources)
