from urllib import urlencode
from werkzeug.datastructures import MultiDict


class Query(object):
    """Hold state for common query parameters."""

    def __init__(self, args, path=None, limit=None):
        if not isinstance(args, MultiDict):
            args = MultiDict(args)
        self.args = args
        self.path = path or ''
        self._limit = limit

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

    def has_param(self, arg, value):
        value = unicode(value).encode('utf-8')
        return (arg, value) in list(self.items())

    def add_param(self, arg, value):
        items = list(self.items)
        value = unicode(value).encode('utf-8')
        items.append((arg, value))
        return self.make_url(items)

    def remove_param(self, arg, value):
        value = unicode(value).encode('utf-8')
        items = [t for t in self.items if t != (arg, value)]
        return self.make_url(items)

    def make_url(self, params):
        if not len(params):
            return self.path
        params = [(k, unicode(v).encode('utf-8')) for (k, v) in params]
        return self.path + '?' + urlencode(params)

    def make_page_url(self, page):
        # TODO: add prefix support
        return self.add_param('offset', (page - 1) * self.limit)

    @property
    def items(self):
        for (k, v) in self.args.iteritems(multi=True):
            # TODO: add prefix support
            if k == 'offset':
                continue
            yield k, v

    def getlist(self, name, default=None):
        if name not in self.args:
            return default or []
        # TODO: add prefix support
        values = self.args.getlist(name)
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
