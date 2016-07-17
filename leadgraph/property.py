import six
import fingerprints


class Property(object):
    # Can apply either to a node or an edge.

    def __init__(self, parent, name, config):
        self.parent = parent
        self.name = name
        self.config = config
        self.column = config.get('column')
        self.literal = config.get('literal')
        self.format = config.get('format')
        self.transform = config.get('transform', '').strip().lower()
        self.key = config.get('key', False)

    def bind(self, row):
        value = row.get(self.column, self.literal)
        if self.format is not None:
            value = self.format % row
        if value is None:
            return
        if self.transform == 'fingerprint':
            value = fingerprints.generate(value)
        if self.transform == 'lowercase' and \
                isinstance(value, six.string_types):
            value = value.lower().strip()
        return value

    def __repr__(self):
        return '<Property(%r)>' % self.name
