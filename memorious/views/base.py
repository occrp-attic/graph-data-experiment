import logging
from flask import render_template, Blueprint, request

from memorious.index import search_entities, Query
from memorious.views.util import dataset_label, entity_schema_label

blueprint = Blueprint('base', __name__)
log = logging.getLogger(__name__)


@blueprint.route('/')
def index():
    return render_template("home.html")


@blueprint.route('/search')
def search():
    query = Query(request.args, path=request.path)
    query.add_facet('schemata', 'Types', entity_schema_label)
    query.add_facet('dataset', 'Dataset', dataset_label)
    results = search_entities(query)
    return render_template("search.html", query=query, results=results)


@blueprint.route('/entities/<id>')
def entity(id):
    entity = {'name': 'Hello, world!'}
    return render_template("entity.html", entity=entity)
