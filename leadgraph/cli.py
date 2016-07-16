import os
import yaml
import click
import logging

from leadgraph.mapping import Mapping
from leadgraph.util import LeadGraphException

log = logging.getLogger(__name__)


@click.command()
@click.argument('mapping', type=click.File('rb'))
def cli(mapping):
    logging.basicConfig(level=logging.DEBUG,
                        format="[%(levelname)-8s] %(message)s")
    logging.getLogger('neo4j').setLevel(level=logging.WARNING)
    logging.getLogger('httpstream').setLevel(level=logging.WARNING)
    config = yaml.load(mapping)
    if not config.get('id'):
        base_name, ext = os.path.splitext(os.path.basename(mapping.name))
        config['id'] = base_name
    try:
        mapping = Mapping(config)
        mapping.load()
    except LeadGraphException as lge:
        log.error(lge)
