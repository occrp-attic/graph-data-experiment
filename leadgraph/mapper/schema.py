

class SchemaProperty(object):

    def __init__(self, schema, name, data):
        self.schema = schema
        self.name = name
        self.data = data

    def __repr__(self):
        return '<SchemaProperty(%r, %r)>' % (self.schema, self.name)


class Schema(object):
    ENTITY = 'entities'
    LINK = 'links'
    SECTIONS = [ENTITY, LINK]

    def __init__(self, section, name, data):
        assert section in self.SECTIONS, section
        self.section = section
        self.name = name
        self.data = data
        self.properties = []
        for name, prop in data.get('properties', {}).items():
            self.properties.append(SchemaProperty(self, name, prop))

    def get(self, name):
        for prop in self.properties:
            if prop.name == name:
                return prop
        raise ValueError("[%r] missing property: %s" % (self, name))

    def __repr__(self):
        return '<Schema(%r)>' % self.name
