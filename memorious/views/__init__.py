import humanize

from memorious.views.assets import assets, compile_assets  # noqa
from memorious.views.base import blueprint as base
from memorious.views.auth import blueprint as auth
from memorious.views.crossref import blueprint as crossref
from memorious.views.util import country, date, cleanurl, normalizeaddress


def mount_app_blueprints(app):
    app.template_filter()(humanize.intcomma)
    app.template_filter()(country)
    app.template_filter()(date)
    app.template_filter()(cleanurl)
    app.template_filter()(normalizeaddress)
    app.register_blueprint(base)
    app.register_blueprint(auth)
    app.register_blueprint(crossref)
    compile_assets(app)
