from urllib import urlencode
from werkzeug.datastructures import MultiDict


class Facet(object):
    """Describe a facet aggregation."""

    def __init__(self, field, label, label_func=None):
        self.field = field
        self.label = label
        self.label_func = label_func


class Query(object):
    """Hold state for common query parameters."""

    def __init__(self, args, prefix='', path=None, limit=None):
        if not isinstance(args, MultiDict):
            args = MultiDict(args)
        self.args = args
        self.prefix = prefix
        self.path = path or ''
        self._limit = limit
        self.facets = []

    def alias(self, arg):
        return self.prefix + arg

    @property
    def limit(self):
        if self._limit is not None:
            return self._limit
        return min(1000, max(0, self.getint('limit', 30)))

    @property
    def offset(self):
        return max(0, self.getint('offset', 0))

    @property
    def page(self):
        if self.limit == 0:
            return 1
        return (self.offset / self.limit) + 1

    @property
    def text(self):
        return self.get('q', '')

    @property
    def has_text(self):
        if self.text is None:
            return False
        return len(self.text.strip()) > 0

    @property
    def has_query(self):
        if self.has_text:
            return True
        # TODO: make this know the actual available filters.
        for item in self.items:
            return True
        return False

    def add_facet(self, field, label, label_func=None):
        self.facets.append(Facet(field, label, label_func=label_func))

    def has_param(self, arg, value):
        value = unicode(value).encode('utf-8')
        return (arg, value) in list(self.items)

    def add_param(self, arg, value):
        items = list(self.items)
        value = unicode(value).encode('utf-8')
        items.append((arg, value))
        return self.make_url(items)

    def remove_param(self, arg, value):
        value = unicode(value).encode('utf-8')
        items = [t for t in self.items if t != (arg, value)]
        return self.make_url(items)

    def toggle_param(self, arg, value):
        if self.has_param(arg, value):
            return self.remove_param(arg, value)
        return self.add_param(arg, value)

    def make_url(self, params):
        if not len(params):
            return self.path
        params = [(self.alias(k), unicode(v).encode('utf-8'))
                  for (k, v) in params]
        return self.path + '?' + urlencode(params)

    def make_page_url(self, page):
        return self.add_param('offset', (page - 1) * self.limit)

    @property
    def items(self):
        for (k, v) in self.args.iteritems(multi=True):
            if not k.startswith(self.prefix):
                continue
            k = k[len(self.prefix):]
            if k == 'offset':
                continue
            yield k, v

    def getlist(self, name, default=None):
        if self.alias(name) not in self.args:
            return default or []
        # TODO: add prefix support
        values = self.args.getlist(self.alias(name))
        if not len(values):
            return default or []
        return values

    def get(self, name, default=None):
        for value in self.getlist(name):
            if len(value.strip()):
                return value
        return default

    def getint(self, name, default=None):
        value = self.get(name, default)
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
