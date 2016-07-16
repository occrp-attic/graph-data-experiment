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
# graph = Graph(password='test')
# graph.delete_all()

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
    def graph(self):
        if not hasattr(self, '_graph'):
            self._graph = Graph(password='test')
            self._graph.delete_all()
        return self._graph

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
        graphtx = self.graph.begin()
        rp = self.engine.execute(self.query)
        while True:
            rows = rp.fetchmany(10000)
            if not len(rows):
                break
            for row in rows:
                self.update(graphtx, dict(row.items()))
        graphtx.commit()

    def update(self, graphtx, row):
        nodes = {}
        for node in self.nodes:
            nodes[node.name] = node.update(graphtx, row)
        for edge in self.edges:
            edge.update(graphtx, row, nodes)

    def __repr__(self):
        return unicode(self.query)


class SubMapping(object):

    def __init__(self, mapping, config):
        self.mapping = mapping
        self.config = config
        self.label = config.get('label')
        assert self.label is not None, "No label defined for node!"
        self._keys = None
        self._indices = None

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

    @property
    def keys(self):
        if self._keys is None:
            self._keys = []
            for prop in self.properties:
                if prop.key:
                    self._keys.append(prop.name)
        return self._keys

    def _prepare_indices(self):
        if self._indices is not None:
            return
        self._indices = self.mapping.graph.schema.get_indexes(self.label)
        for key in self.keys:
            if key not in self._indices:
                self.mapping.graph.schema.create_index(self.label, key)
        self._indices = True

    def save(self, graphtx, subgraph, props):
        self._prepare_indices()
        keys = [k for k in self.keys if subgraph.get(k)]
        graphtx.merge(subgraph, self.label, *keys)


class NodeMapping(SubMapping):

    def __init__(self, mapping, name, config):
        self.name = name
        super(NodeMapping, self).__init__(mapping, config)

    def update(self, graphtx, row):
        props = self.bind_properties(row)
        node = Node(self.label, **props)
        self.save(graphtx, node, props)
        return node

    def __repr__(self):
        return '<NodeMapping(%r)>' % self.name


class EdgeMapping(SubMapping):

    def __init__(self, mapping, config):
        self.source = config.get('source')
        assert self.source is not None, "No source defined for edge!"
        self.target = config.get('target')
        assert self.target is not None, "No target defined for edge!"
        super(EdgeMapping, self).__init__(mapping, config)

    def update(self, graphtx, row, nodes):
        props = self.bind_properties(row)
        source = nodes[self.source]
        target = nodes[self.target]
        if source is None or target is None:
            print "Missing node", row
            return
        rel = Relationship(source, self.label, target, **props)
        self.save(graphtx, rel, props)
        # print source, target, props

    def __repr__(self):
        return '<EdgeMapping(%r, %r, %r)>' % \
            (self.source, self.label, self.target)


class Property(object):

    def __init__(self, parent, name, config):
        self.parent = parent
        self.name = name
        self.config = config
        self.column = config.get('column')
        self.literal = config.get('literal')
        self.transform = config.get('transform', '').strip().lower()
        self.key = config.get('key', False)

    def bind(self, row):
        value = row.get(self.column, self.literal)
        if self.transform == 'fingerprint':
            value = fingerprints.generate(value)
        return value

    def __repr__(self):
        return '<Property(%r)>' % self.name


with open('mapping.yaml', 'r') as fh:
    config = yaml.load(fh)

mapping = Mapping(config)
mapping.load()
