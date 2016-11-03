import math
import logging

from memorious.core import model
from memorious.model import Schema

log = logging.getLogger(__name__)


class ResultDocument(object):

    def __init__(self, document, parent=None):
        self.parent = parent
        self.document = document
        self.data = document.get('_source')
        self.id = document.get('_id')
        self.properties = self.data.get('properties')
        self.schema = model.get_schema(document.get('_type'),
                                       self.data.get('schema'))
        self.dataset = model.get_dataset(self.data.get('dataset'))

    def list_properties(self):
        listed = []
        for name, values in self.properties.items():
            try:
                prop = self.schema.get(name)
            except ValueError as ve:
                log.info("Invalid property: %s", ve)
                continue
            if not prop.is_hidden and len(values):
                listed.append((prop, values))
        return sorted(listed, key=lambda (p, v): p.label)

    def has_properties(self):
        return len(list(self.list_properties())) > 0


class EntityResult(ResultDocument):

    def __init__(self, document, parent=None):
        super(EntityResult, self).__init__(document, parent=parent)
        self.name = self.data.get('name')


class LinkResult(ResultDocument):

    def __init__(self, document, parent=None):
        super(LinkResult, self).__init__(document, parent=parent)
        self.remote = self.data.get('remote')
        self.origin = self.data.get('origin')
        self.inverted = self.data.get('inverted')
        if self.inverted:
            self.label = self.schema.reverse
        else:
            self.label = self.schema.forward


class FacetBucket(object):

    def __init__(self, facet, bucket):
        self.facet = facet
        self.key = bucket.get('key')
        self.count = bucket.get('doc_count')

    @property
    def label(self):
        if self.facet.label_func is None:
            return self.key
        return self.facet.label_func(self.key)

    def __len__(self):
        return self.count


class ResultSet(object):

    def __init__(self, query, results, parent=None):
        self.query = query
        self.results = results
        self.parent = parent
        self.hits = results.get('hits', {})
        self.aggregations = results.get('aggregations', {})
        self.total = self.hits.get('total', 0)

    @property
    def pages(self):
        if self.query.limit == 0:
            return 1
        return int(math.ceil(self.total / float(self.query.limit)))

    @property
    def has_next(self):
        return self.query.page < self.pages

    @property
    def has_prev(self):
        return self.query.page > 1

    @property
    def next_url(self):
        if not self.has_next:
            return ''
        return self.query.make_page_url(self.query.page + 1)

    @property
    def prev_url(self):
        if not self.has_prev:
            return ''
        return self.query.make_page_url(self.query.page - 1)

    def pager(self, pager_range=4):
        low = self.query.page - pager_range
        high = self.query.page + pager_range

        if low < 1:
            low = 1
            high = min((2 * pager_range) + 1, self.pages)

        if high > self.pages:
            high = self.pages
            low = max(1, self.pages - (2 * pager_range) + 1)

        for page in range(low, high + 1):
            yield page, self.query.make_page_url(page), page == self.query.page

    @property
    def facets(self):
        for facet in self.query.facets:
            data = self.aggregations.get(facet.field, {})
            # This is a bit ugly, nailing the buckets onto the facet externally
            facet.buckets = []
            for bucket in data.get('buckets', []):
                facet.buckets.append(FacetBucket(facet, bucket))
            yield facet

    def __iter__(self):
        for document in self.hits.get('hits', []):
            if document.get('_type') == Schema.ENTITY:
                yield EntityResult(document, parent=self.parent)
            elif document.get('_type') == Schema.LINK:
                yield LinkResult(document, parent=self.parent)

    def __len__(self):
        return self.total

    def __repr__(self):
        return '<Result(%r)>' % (self.total)
