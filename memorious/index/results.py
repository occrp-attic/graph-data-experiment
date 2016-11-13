import math
import logging

from memorious.core import model
from memorious.model import Schema

log = logging.getLogger(__name__)


class ResultDocument(object):

    def __init__(self, document, parent=None):
        self.parent = parent
        self.document = document
        self.data = document.get('_source', document)
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
        self.fingerprints = self.data.get('fingerprints', [])


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
        rschema = self.remote.get('schema')
        self.remote_schema = model.get_schema(Schema.ENTITY, rschema)


class FacetBucket(object):

    def __init__(self, facet, bucket, active):
        self.facet = facet
        self.key = bucket.get('key')
        self.count = bucket.get('doc_count')
        self.active = active

    @property
    def label(self):
        if self.facet.label_func is None:
            return self.key
        return self.facet.label_func(self.key)

    @property
    def icon(self):
        if self.facet.icon_func is not None:
            return self.facet.icon_func(self.key)

    def __cmp__(self, other):
        val = cmp(self.active, other.active)
        if val == 0:
            val = cmp(self.count, other.count)
        return val


class FacetResult(object):

    def __init__(self, result, facet):
        self.result = result
        self.facet = facet
        self.field = facet.field
        self.active = result.query.getlist(facet.field)
        aggregation = result.aggregations.get(facet.field, {})
        self._buckets = aggregation.get('buckets')

    @property
    def show(self):
        return len(self.active) or len(self._buckets)

    @property
    def buckets(self):
        seen = set()
        buckets = []
        for bucket in self._buckets:
            key = bucket.get('key')
            active = key in self.active
            seen.add(key)
            buckets.append(FacetBucket(self.facet, bucket, active))
        for active in self.active:
            if active in seen:
                continue
            bucket = {'key': active, 'doc_count': 0}
            buckets.append(FacetBucket(self.facet, bucket, True))
        return sorted(buckets, reverse=True)


class ResultSet(object):

    def __init__(self, query, results, parent=None):
        self.query = query
        self.results = results
        self.parent = parent
        self.hits = results.get('hits', {})
        self.aggregations = results.get('aggregations', {})
        self.total = self.hits.get('total', 0)

    @property
    def show_results(self):
        return self.query.has_query or self.total > 0

    @property
    def show_filters(self):
        return self.query.has_query or self.total > self.query.limit

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
        return [FacetResult(self, f) for f in self.query.facets]

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


class CrossrefResult(ResultSet):

    def __init__(self, query, results, sub_results):
        super(CrossrefResult, self).__init__(query, results)
        self.sub_results = sub_results

    def __iter__(self):
        yield 'hello'
        yield 'hello 2'
        yield 'hello 3'
