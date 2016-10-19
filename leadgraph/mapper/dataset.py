import six
import logging
from sqlalchemy import select
from sqlalchemy.schema import Table

from leadgraph.core import meta, engine
from leadgraph.util import DATA_PAGE
from leadgraph.mapper.mapper import EntityMapper, LinkMapper
from leadgraph.mapper.record import Record

log = logging.getLogger(__name__)


class DatasetTable(object):
    """A table to be joined in."""

    def __init__(self, dataset, data):
        self.dataset = dataset
        if isinstance(data, six.string_types):
            data = {'table': data}
        self.data = data
        self.table_ref = data.get('table')
        self.alias_ref = data.get('alias', self.table_ref)
        self.table = Table(self.table_ref, meta, autoload=True)
        self.alias = self.table.alias(self.alias_ref)

    @property
    def refs(self):
        if not hasattr(self, '_refs'):
            self._refs = {}
            for column in self.alias.columns:
                name = '%s.%s' % (self.alias_ref, column.name)
                self._refs[name] = column
                self._refs[column.name] = column
        return self._refs

    def __repr__(self):
        return '<ViewTable(%r,%r)>' % (self.alias_ref, self.table_ref)


class DatasetField(object):
    """A field to be included in loaded data."""

    def __init__(self, model, view, data):
        self.model = model
        self.view = view
        self.data = data
        self.label = data.get('label', self.column_ref)
        self.column_ref = data.get('column')
        self.column = view.get_column(self.column_ref)
        self.table = view.get_table(self.column_ref)

    def __repr__(self):
        return '<ViewField(%r)>' % self.column_ref


class Dataset(object):
    """A dataset describes one set of data to be loaded."""

    def __init__(self, model, name, data):
        self.model = model
        self.name = six.text_type(name)
        self.label = data.get('label', name)
        self.data = data
        tables = data.get('tables', [data.get('table')])
        self.tables = [DatasetTable(self, f) for f in tables]
        self.fields = [DatasetField(self, f) for f in data.get('fields', [])]

        self.entities = []
        for ename, edata in data.get('entities').items():
            self.entities.append(EntityMapper(self, ename, edata))

        self.links = []
        for ldata in data.get('links', []):
            self.links.append(LinkMapper(self, ldata))

    def get_column(self, ref):
        for table in self.tables:
            if ref in table.refs:
                return table.refs.get(ref)
        raise TypeError("[%r] Cannot find column: %s" % (self, ref))

    def get_table(self, ref):
        for table in self.tables:
            if ref == table.alias_ref:
                return table
        raise TypeError("[%r] Cannot find table: %s" % (self, ref))

    @property
    def from_clause(self):
        return [t.alias for t in self.tables]

    @property
    def mapped_columns(self):
        refs = set()
        for item in self.entities + self.links:
            for ref in item.refs:
                refs.add(ref)
        return [self.get_column(r) for r in refs]

    def apply_filters(self, q):
        for col, val in self.data.get('filters', {}).items():
            q = q.where(self.get_column(col) == val)
        for join in self.data.get('joins', []):
            left = self.get_column(join.get('left'))
            right = self.get_column(join.get('right'))
            q = q.where(left == right)
        return q

    def iterrecords(self):
        q = select(columns=self.mapped_columns, from_obj=self.from_clause)
        q = self.apply_filters(q)
        log.info("Query [%s]: %s", self.name, q)
        rp = engine.execute(q)
        while True:
            rows = rp.fetchmany(DATA_PAGE)
            if not len(rows):
                break
            for row in rows:
                yield Record(self, row)

    def __repr__(self):
        return '<Dataset(%r)>' % self.name
