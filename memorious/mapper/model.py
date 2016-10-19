from memorious.mapper.schema import Schema
from memorious.mapper.dataset import Dataset


class Model(object):

    def __init__(self, data):
        self.data = data

        self.schemata = []
        for section in Schema.SECTIONS:
            for name, sconfig in data['schema'].get(section, {}).items():
                self.schemata.append(Schema(self, section, name, sconfig))

        self.datasets = []
        for name, dconfig in data.get('datasets', {}).items():
            self.datasets.append(Dataset(self, name, dconfig))

    def get_schema(self, section, name):
        for schema in self.schemata:
            if schema.section == section and schema.name == name:
                return schema
        raise TypeError("No such schema for %s: %s" % (section, name))

    def __repr__(self):
        return '<Model(%r, %r)>' % (self.schemata, self.datasets)
