import fingerprints


class Property(object):
    # Can apply either to a node or an edge.

    def __init__(self, parent, name, config):
        self.parent = parent
        self.name = name
        self.config = config
        self.column = config.get('column')
        self.literal = config.get('literal')
        self.transform = config.get('transform', '').strip().lower()
        self.key = config.get('key', False)

    def bind(self, row):
        value = row.get(self.column, self.literal)
        if self.transform == 'fingerprint':
            value = fingerprints.generate(value)
        return value

    def __repr__(self):
        return '<Property(%r)>' % self.name
