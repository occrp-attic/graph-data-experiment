import os
import logging
from py2neo import Graph
from collections import defaultdict
from sqlalchemy import MetaData, create_engine
from sqlalchemy.schema import Table

from leadgraph.node import NodeMapping
from leadgraph.edge import EdgeMapping
from leadgraph.util import LeadGraphException

log = logging.getLogger(__name__)


class Mapping(object):
    """Main mapping executor, config and data access."""

    def __init__(self, config):
        self.config = config

    def get_env_config(self, name, env_name, default=None):
        value = self.config.get(name)
        if value is not None:
            value = os.path.expandvars(value)
        else:
            value = os.environ.get(env_name, default)
        if value is None:
            raise LeadGraphException("Missing option: %s" % name)
        return value

    @property
    def graph(self):
        """Instantiate the Neo4J graph connector."""
        if not hasattr(self, '_graph'):
            host = self.get_env_config('graph_host',
                                       'NEO4J_HOST',
                                       'localhost')
            user = self.get_env_config('graph_user',
                                       'NEO4J_USER',
                                       'neo4j')
            password = self.get_env_config('graph_password',
                                           'NEO4J_PASSWORD',
                                           'neo4j')
            self._graph = Graph(host=host, user=user, password=password)
            log.info('Destination: %r', self._graph)
        return self._graph

    def clear(self):
        log.info("Deleting existing data from graph...")
        self.graph.delete_all()

    @property
    def engine(self):
        """Instantiate the SQL database client."""
        if not hasattr(self, '_engine'):
            database_uri = self.get_env_config('database', 'DATABASE_URI')
            self._engine = create_engine(database_uri)
        return self._engine

    @property
    def meta(self):
        """Get SQLAlechemy table metadata."""
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
        log.info("Query: %s", query)
        return query

    @property
    def nodes(self):
        if not hasattr(self, '_nodes'):
            self._nodes = []
            for name, config in self.config.get('nodes', {}).items():
                self._nodes.append(NodeMapping(self, name, config))
            if not len(self._nodes):
                raise LeadGraphException("No nodes defined in mapping.")
        return self._nodes

    @property
    def edges(self):
        if not hasattr(self, '_edges'):
            self._edges = []
            for config in self.config.get('edges', []):
                self._edges.append(EdgeMapping(self, config))
        return self._edges

    def load(self):
        """Generate query rows and load them into the graph."""
        # This is all wrapped in a transaction. Makes it faster,
        # but should the transaction be the full dataset?
        rp = self.engine.execute(self.query)
        stats = defaultdict(int)
        while True:
            graphtx = self.graph.begin()
            rows = rp.fetchmany(10000)
            if not len(rows):
                break
            for row in rows:
                stats['rows'] += 1
                self.update(graphtx, dict(row.items()), stats)
                if stats['rows'] % 1000 == 0:
                    log.info("Loaded: %(rows)s [%(nodes)s nodes, "
                             "%(rels)s edges]", stats)
            graphtx.commit()
        log.info("Done. Loaded %(rows)s rows, %(nodes)s nodes, "
                 "%(rels)s edges.", stats)

    def update(self, graphtx, row, stats):
        """Generate nodes and edges for a single row."""
        nodes = {}
        for node in self.nodes:
            nodes[node.name] = node.update(graphtx, row)
            if nodes[node.name] is not None:
                stats['nodes'] += 1
        for edge in self.edges:
            rel = edge.update(graphtx, row, nodes)
            if rel is not None:
                stats['rels'] += 1

    def __repr__(self):
        return unicode(self.query)
