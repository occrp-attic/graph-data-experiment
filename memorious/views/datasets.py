import logging
from werkzeug.exceptions import NotFound, Forbidden
from flask import render_template, Blueprint, request

from memorious.core import model
from memorious.index import Query
from memorious.index.crossref import search_datasets, search_crossref_entities

blueprint = Blueprint('datasets', __name__)
log = logging.getLogger(__name__)


def get_dataset(name):
    try:
        dataset = model.get_dataset(name)
        if not request.auth.has_access(dataset):
            raise Forbidden("Not auhtorized to access: %s" % name)
        return dataset
    except TypeError:
        raise NotFound("No such dataset: %s" % name)


@blueprint.route('/datasets/<dataset>')
def view(dataset):
    dataset = get_dataset(dataset)
    crossrefs = search_datasets([dataset.name])
    return render_template('dataset.html', dataset=dataset,
                           crossrefs=crossrefs)


@blueprint.route('/datasets/<dataset>/crossref/<other>')
def crossref(dataset, other):
    dataset, other = get_dataset(dataset), get_dataset(other)
    query = Query(request.args, path=request.path)
    matches = search_crossref_entities([dataset.name, other.name], query)
    return render_template('crossref.html', dataset=dataset, other=other,
                           matches=matches)
