# coding: utf-8
import logging
from flask_script import Manager

from memorious.core import create_app, model
from memorious.views import mount_app_blueprints
from memorious.index import delete_dataset, init_search
from memorious.loader import load_dataset

log = logging.getLogger(__name__)
app = create_app()
mount_app_blueprints(app)
manager = Manager(app)


@manager.command
def init():
    """Re-initialise the search index."""
    init_search()


@manager.command
def index():
    """Index all datasets."""
    for dataset in model.datasets:
        load_dataset(dataset)


@manager.command
def load(name):
    """Index all the entities in a given dataset."""
    dataset = model.get_dataset(name)
    load_dataset(dataset)


@manager.command
def delete(name):
    delete_dataset(name)


def main():
    manager.run()


if __name__ == "__main__":
    main()
