import os
import yaml
import fingerprints
from py2neo import Graph, Node, Relationship
from sqlalchemy import MetaData, select, func, create_engine
# from sqlalchemy import Unicode, Float
# afrom sqlalchemy.sql.expression import cast
from sqlalchemy.schema import Table, Column, Index


## setup db
# engine = create_engine('postgresql://localhost/datavault')
# meta = MetaData()
# meta.bind = engine
# meta.reflect()
# table = meta.tables['az_tenders']
# res = engine.execute(table.select())
# rows = res.fetchall()


## setup graph
graph = Graph(password='test')
graph.delete_all()

# Identifier = 'Identifier'
# Actor = 'Actor'
# IDENTIFIED_BY = 'IDENTIFIED_BY'

# graph.schema.create_index(Identifier, 'value')
# graph.schema.create_index(Actor, 'actorName')
# graph.schema.drop_index(Identifier, 'value')
# graph.schema.create_uniqueness_constraint(Identifier, 'value')

# actor = Node(Actor, actorName='Friedrich Lindenberg')
# identifier = Node(Identifier, value='friedrich-lindenberg')
# rel = Relationship(actor, IDENTIFIED_BY, identifier)
# sg = actor | identifier | rel

# print sg
# graph.create(sg)

# actor = Node(Actor, actorName='Friedrich Lindenberg')
# identifier = Node(Identifier, value='friedrich-lindenberg')
# rel = Relationship(actor, IDENTIFIED_BY, identifier)
# sg = actor | identifier | rel
# print sg
# graph.merge(sg, Identifier, 'value')
# print sg

# actor = Node(Actor, actorName='Friedrich Lindenberg', ids=['friedrich-lindenberg'])
# graph.merge(actor, Actor, 'ids')
# actor = Node(Actor, actorName='Friedrich Lindenberg', ids=['friedrich-lindenberg', 'pudo'])
# graph.merge(actor, Actor, 'ids')


class Mapping(object):

    def __init__(self, config):
        self.config = config

    @property
    def engine(self):
        if not hasattr(self, '_engine'):
            database_uri = self.config.get('database')
            if database_uri is not None:
                database_uri = os.path.expandvars(database_uri)
            database_uri = database_uri or os.environ.get('DATABASE_URI')
            self._engine = create_engine(database_uri)
        return self._engine

    @property
    def meta(self):
        if not hasattr(self, '_meta'):
            self._meta = MetaData()
            self._meta.bind = self.engine
        return self._meta

    @property
    def query(self):
        query = self.config.get('query')
        if not query:
            table_name = self.config.get('table')
            table = Table(table_name, self.meta, autoload=True)
            query = table.select()
        return query

    @property
    def nodes(self):
        if not hasattr(self, '_nodes'):
            self._nodes = []
            for name, config in self.config.get('nodes', {}).items():
                self._nodes.append(NodeMapping(self, name, config))
            assert len(self._nodes), "No nodes defined!"
        return self._nodes

    @property
    def edges(self):
        if not hasattr(self, '_edges'):
            self._edges = []
            for config in self.config.get('edges', []):
                self._edges.append(EdgeMapping(self, config))
        return self._edges

    def load(self):
        rp = self.engine.execute(self.query)
        while True:
            rows = rp.fetchmany(10000)
            if not len(rows):
                break
            for row in rows:
                self.update(dict(row.items()))

    def update(self, row):
        nodes = {}
        for node in self.nodes:
            nodes[node.name] = node.update(row)
        for edge in self.edges:
            edge.update(row, nodes)

    def __repr__(self):
        return unicode(self.query)


class SubMapping(object):

    @property
    def properties(self):
        if not hasattr(self, '_properties'):
            self._properties = []
            for name, config in self.config.get('properties', {}).items():
                self._properties.append(Property(self, name, config))
        return self._properties

    def bind_properties(self, row):
        props = {}
        for prop in self.properties:
            value = prop.bind(row)
            if value is not None:
                props[prop.name] = value
        return props


class NodeMapping(SubMapping):

    def __init__(self, mapping, name, config):
        self.mapping = mapping
        self.name = name
        self.config = config
        self.label = config.get('label')
        assert self.label is not None, "No label defined for node!"

    def update(self, row):
        props = self.bind_properties(row)
        node = Node(self.label, **props)
        graph.create(node)
        return node

    def __repr__(self):
        return '<NodeMapping(%r)>' % self.name


class EdgeMapping(SubMapping):

    def __init__(self, mapping, config):
        self.mapping = mapping
        self.config = config
        self.type = config.get('type')
        assert self.type is not None, "No type defined for edge!"
        self.source = config.get('source')
        assert self.source is not None, "No source defined for edge!"
        self.target = config.get('target')
        assert self.target is not None, "No target defined for edge!"

    def update(self, row, nodes):
        props = self.bind_properties(row)
        source = nodes[self.source]
        target = nodes[self.target]
        print source, target, props

    def __repr__(self):
        return '<EdgeMapping(%r, %r, %r)>' % \
            (self.source, self.type, self.target)


class Property(object):

    def __init__(self, parent, name, config):
        self.parent = parent
        self.name = name
        self.config = config
        self.column = config.get('column')
        self.transform = config.get('transform', '').strip().lower()
        self.key = config.get('key', False)

    def bind(self, row):
        value = row.get(self.column)
        if self.transform == 'fingerprint':
            value = fingerprints.generate(value)
        return value

    def __repr__(self):
        return '<Property(%r)>' % self.name


with open('mapping.yaml', 'r') as fh:
    config = yaml.load(fh)

mapping = Mapping(config)
mapping.load()
