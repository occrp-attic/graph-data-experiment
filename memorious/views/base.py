import logging
from werkzeug.exceptions import NotFound
from flask import render_template, Blueprint, request

from memorious.index import search_entities, search_links
from memorious.index import load_entity, Query
from memorious.views.util import dataset_label, entity_schema_label
from memorious.views.util import country_label, link_schema_label

blueprint = Blueprint('base', __name__)
log = logging.getLogger(__name__)


@blueprint.route('/')
def index():
    return render_template("home.html")


@blueprint.route('/search')
def search():
    query = Query(request.args, path=request.path)
    query.add_facet('schemata', 'Types', entity_schema_label)
    query.add_facet('countries', 'Countries', country_label)
    query.add_facet('dataset', 'Dataset', dataset_label)
    results = search_entities(query, request.auth)
    return render_template("search.html", query=query, results=results)


@blueprint.route('/entities/<entity_id>')
def entity(entity_id):
    entity = load_entity(entity_id, request.auth)
    if entity is None:
        raise NotFound()

    # load links
    query = Query(request.args, prefix='links_', path=request.path)
    query.add_facet('schemata', 'Types', link_schema_label)
    query.add_facet('remote.countries', 'Countries', country_label)
    links = search_links(entity, query, request.auth)

    links.visible = links.total > 0 or query.has_query
    links.show_filters = links.total > query.limit or query.has_query

    return render_template("entity.html", entity=entity, links=links)
