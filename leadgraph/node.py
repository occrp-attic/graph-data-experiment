import logging
from py2neo import Node

from leadgraph.item import ItemMapping
from leadgraph.util import LeadGraphException, has_value

log = logging.getLogger(__name__)


class NodeMapping(ItemMapping):
    """Map columns from the source data to a graph node."""

    def __init__(self, mapping, name, config):
        self.name = name
        super(NodeMapping, self).__init__(mapping, config)
        if not len(self.keys):
            raise LeadGraphException("No keys defined for node: %r" % name)

    def update(self, graphtx, row):
        """Prepare and load a node."""
        props = self.bind_properties(row)
        props['sourceId'] = self.mapping.config.get('id')
        keys = [k for k in self.keys if has_value(props.get(k))]
        if not len(keys):
            # log.warning("No key for node: %r", row)
            return
        node = Node(self.label, **props)
        self.save(graphtx, node, props)
        return node

    def __repr__(self):
        return '<NodeMapping(%r)>' % self.name
