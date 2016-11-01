# coding: utf-8
import logging
from flask_script import Manager

from memorious.core import create_app, model
from memorious.views import mount_app_blueprints
from memorious.index import index_dataset, delete_dataset, init_search

log = logging.getLogger(__name__)
app = create_app()
mount_app_blueprints(app)
manager = Manager(app)


def sub_opts(app, **kwargs):
    print app, kwargs
    pass

dataset_manager = Manager(sub_opts, help="Dataset-related operations",
                          description="Operations which relate to a dataset")


@manager.command
def init():
    """Re-initialise the search index."""
    init_search()


@manager.command
def index():
    """Index all datasets."""
    for dataset in model.datasets:
        index_dataset(dataset)


@dataset_manager.command
def load(name):
    """Index all the entities in a given dataset."""
    dataset = model.get_dataset(name)
    index_dataset(dataset)


@dataset_manager.command
def delete(name):
    dataset = model.get_dataset(name)
    delete_dataset(dataset)


def main():
    manager.add_command("dataset", dataset_manager)
    manager.run()


if __name__ == "__main__":
    main()
