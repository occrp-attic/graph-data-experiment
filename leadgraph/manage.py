# coding: utf-8
import logging

from flask_script import Manager

from leadgraph.core import create_app

log = logging.getLogger(__name__)
app = create_app()
manager = Manager(app)


@manager.command
def load():
    pass


def main():
    manager.run()


if __name__ == "__main__":
    main()
