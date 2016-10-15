import logging

from leadgraph.property import Property
from leadgraph.util import LeadGraphException

log = logging.getLogger(__name__)


class ItemMapping(object):

    def __init__(self, mapping, config):
        self.mapping = mapping
        self.config = config
        self.label = config.get('label')
        self.country = config.get('country')
        if self.label is None:
            raise LeadGraphException("No label defined for node: %r!" % self)

    @property
    def properties(self):
        if not hasattr(self, '_properties'):
            self._properties = []
            for name, config in self.config.get('properties', {}).items():
                self._properties.append(Property(self, name, config))
        return self._properties

    def bind_properties(self, row):
        """Fill graph properties from source row."""
        props = {}
        for prop in self.properties:
            value = prop.bind(row)
            if value is not None:
                props[prop.name] = value
        return props

    @property
    def keys(self):
        """Return names of all properties designated as keys."""
        if not hasattr(self, '_keys'):
            self._keys = []
            for prop in self.properties:
                if prop.key:
                    self._keys.append(prop.name)
        return self._keys

    def save(self, graphtx, subgraph, props):
        keys = [k for k in self.keys if subgraph.get(k)]
        graphtx.merge(subgraph, self.label, tuple(keys))
