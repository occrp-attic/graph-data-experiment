# coding: utf-8
import logging
from flask_script import Manager

from leadgraph.core import create_app, model
from leadgraph.views import mount_app_blueprints
from leadgraph.index import index_dataset, init_search

log = logging.getLogger(__name__)
app = create_app()
mount_app_blueprints(app)
manager = Manager(app)


@manager.command
def index():
    for dataset in model.datasets:
        index_dataset(dataset)


@manager.command
def reset():
    init_search()


def main():
    manager.run()


if __name__ == "__main__":
    main()
