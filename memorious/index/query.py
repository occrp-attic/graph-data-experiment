from werkzeug.datastructures import MultiDict


class Query(object):
    """Hold state for common query parameters."""

    def __init__(self, args, limit=None):
        if not isinstance(args, MultiDict):
            args = MultiDict(args)
        self.args = args
        self._limit = limit

    @property
    def limit(self):
        if self._limit is not None:
            return self._limit
        return min(1000, max(0, self.getint('limit', 50)))

    @property
    def offset(self):
        return max(0, self.getint('offset', 0))

    @property
    def text(self):
        return self.get('q', '')

    @property
    def has_text(self):
        if self.text is None:
            return False
        return len(self.text.strip()) > 0

    def getlist(self, name, default=None):
        if name not in self.args:
            return default or []
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
