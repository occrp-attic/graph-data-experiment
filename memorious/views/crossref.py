import logging
from flask import render_template, Blueprint, request

from memorious.core import model
from memorious.index import Query
from memorious.index.crossref import search_datasets, search_crossref_entities

blueprint = Blueprint('crossref', __name__)
log = logging.getLogger(__name__)


@blueprint.route('/crossref')
def datasets():
    datasets = []
    for dataset in request.args.getlist('dataset'):
        try:
            dataset = model.get_dataset(dataset)
        except TypeError as te:
            log.exception(te)
        if request.auth.has_access(dataset):
            datasets.append(dataset)

    names = [d.name for d in datasets]
    counts = search_datasets(names)

    matches = None
    if len(datasets) > 1:
        query = Query(request.args, path=request.path)
        matches = search_crossref_entities(names, query)
    return render_template('crossref/index.html', datasets=datasets,
                           counts=counts, matches=matches)
