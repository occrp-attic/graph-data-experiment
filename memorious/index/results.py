from memorious.core import model
from memorious.mapper.schema import Schema


class ResultDocument(object):

    def __init__(self, result_set, document):
        self.result_set = result_set
        self.document = document
        self.data = document.get('_source')
        self.id = document.get('_id')
        self.properties = self.data.get('properties')
        self.schema = model.get_schema(document.get('_type'),
                                       self.data.get('schema'))


class EntityResult(ResultDocument):

    def __init__(self, result_set, document):
        super(EntityResult, self).__init__(result_set, document)
        self.name = self.data.get('name')


class ResultSet(object):

    def __init__(self, query, results):
        self.query = query
        self.results = results
        self.hits = results.get('hits', {})
        self.total = self.hits.get('total', 0)

    def __iter__(self):
        for document in self.hits.get('hits', []):
            if document.get('_type') == Schema.ENTITY:
                yield EntityResult(self, document)
            else:
                yield document

    def __repr__(self):
        return '<Result(%r)>' % (self.total)
