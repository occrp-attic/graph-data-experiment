import logging
from datetime import datetime, date

from leadgraph.transforms import TRANSFORMS
from leadgraph.util import LeadGraphException


log = logging.getLogger(__name__)


class Property(object):
    # Can apply either to a node or an edge.

    def __init__(self, item, name, config):
        self.item = item
        self.name = name
        self.config = config
        self.key = config.get('key', False)
        self.column = config.get('column')
        self.literal = config.get('literal')
        self.format = config.get('format')
        self.nulls = config.get('nulls', [])
        self.country = config.get('country', self.item.country)
        self.transforms = config.get('transforms', [])
        if config.get('transform'):
            self.transforms.append(config.get('transform'))

    def bind(self, row):
        value = row.get(self.column, self.literal)
        if self.format is not None:
            value = self.format % row
        if value in self.nulls:
            return None
        if value is None:
            return self.literal
        for transform in self.transforms:
            if transform not in TRANSFORMS:
                raise LeadGraphException("No such transformer: %r" % transform)
            value = TRANSFORMS[transform](value, row=row, prop=self)
            if value is None:
                break
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            value = value.isoformat()
        return value

    def __repr__(self):
        return '<Property(%r)>' % self.name
