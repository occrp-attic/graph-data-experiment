# coding: utf-8
import logging
from flask_script import Manager

from leadgraph.core import create_app, model
from leadgraph.index import index_source, init_search

log = logging.getLogger(__name__)
app = create_app()
manager = Manager(app)


@manager.command
def index():
    for source in model.sources:
        index_source(source)


@manager.command
def reset():
    init_search()


def main():
    manager.run()


if __name__ == "__main__":
    main()
