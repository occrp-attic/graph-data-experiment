import logging
from flask import render_template, Blueprint, request

from memorious.index import search_entities, Query

blueprint = Blueprint('base', __name__)
log = logging.getLogger(__name__)


@blueprint.route('/')
def index():
    return render_template("home.html")


@blueprint.route('/search')
def search():
    query = Query(request.args)
    results = search_entities(query)
    return render_template("search.html", query=query, results=results)
