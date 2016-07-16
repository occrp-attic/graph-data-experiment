import logging
from py2neo import Relationship

from leadgraph.item import ItemMapping
from leadgraph.util import LeadGraphException

log = logging.getLogger(__name__)


class EdgeMapping(ItemMapping):
    """Map columns from the source data to a graph edge."""

    def __init__(self, mapping, config):
        self.source = config.get('source')
        if self.source is None:
            raise LeadGraphException("No source for edge: %r" % config)
        self.target = config.get('target')
        if self.target is None:
            raise LeadGraphException("No target for edge: %r" % config)
        super(EdgeMapping, self).__init__(mapping, config)

    def update(self, graphtx, row, nodes):
        """Prepare and load a graph edge."""
        props = self.bind_properties(row)
        source = nodes.get(self.source)
        if source is None:
            log.warning("No %r source node, skipping: %s", self.source, row)
            return
        target = nodes.get(self.target)
        if target is None:
            log.warning("No %r target node, skipping: %s", self.target, row)
            return
        rel = Relationship(source, self.label, target, **props)
        self.save(graphtx, rel, props)
        return rel

    def __repr__(self):
        return '<EdgeMapping(%r, %r, %r)>' % \
            (self.source, self.label, self.target)
