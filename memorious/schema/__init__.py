from memorious.schema.types import resolve_type
from memorious.util import dict_list


class SchemaProperty(object):

    def __init__(self, schema, name, data):
        self.schema = schema
        self.name = name
        self.data = data
        self.is_label = name == 'name'
        self.type_cls = resolve_type(data.get('type', 'string'))

    def __repr__(self):
        return '<SchemaProperty(%r, %r)>' % (self.schema, self.name)


class Schema(object):
    """Defines the abstract data model.

    Schema items define the entities and links available in the model.
    """

    ENTITY = 'entities'
    LINK = 'links'
    SECTIONS = [ENTITY, LINK]

    def __init__(self, model, section, name, data):
        assert section in self.SECTIONS, section
        self.model = model
        self.section = section
        self.name = name
        self.data = data
        self.label = data.get('label', name)
        self.is_hidden = data.get('hidden', False)
        self._extends = dict_list(data, 'extends')
        self._own_properties = []
        for name, prop in data.get('properties', {}).items():
            self._own_properties.append(SchemaProperty(self, name, prop))

    @property
    def extends(self):
        """Return the inherited schemata."""
        for base in self._extends:
            yield self.model.get_schema(self.section, base)

    @property
    def schemata(self):
        """Return the full inheritance chain."""
        if not self.is_hidden:
            yield self.name
        for base in self.extends:
            for name in base.schemata:
                yield name

    @property
    def properties(self):
        """Return properties, those defined locally and in ancestors."""
        for prop in self._own_properties:
            yield prop
        for schema in self.extends:
            for prop in schema.properties:
                yield prop

    def get(self, name):
        for prop in self.properties:
            if prop.name == name:
                return prop
        raise ValueError("[%r] missing property: %s" % (self, name))

    def __repr__(self):
        return '<Schema(%r)>' % self.name
