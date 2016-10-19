import logging
from flask import session, Blueprint, redirect, request
from flask_oauthlib.client import OAuthException
from werkzeug.exceptions import Unauthorized

from memorious.core import url_for, oauth_provider
from memorious.views.util import is_safe_url


log = logging.getLogger(__name__)
blueprint = Blueprint('auth', __name__)


@oauth_provider.tokengetter
def get_oauth_token():
    if 'oauth' in session:
        sig = session.get('oauth')
        return (sig.get('access_token'), '')


@blueprint.before_app_request
def prepare_auth():
    print session.get('user')


@blueprint.route('/login')
def login():
    next_url = '/'
    for target in request.args.get('next'), request.referrer:
        if not target:
            continue
        if is_safe_url(target):
            next_url = target
    session['next_url'] = next_url
    return oauth_provider.authorize(callback=url_for('.callback'))


@blueprint.route('/logout')
def logout():
    session.clear()
    return redirect('/')


@blueprint.route('/api/1/sessions/callback')
def callback():
    resp = oauth_provider.authorized_response()
    if resp is None or isinstance(resp, OAuthException):
        log.warning("Failed OAuth: %r", resp)
        return Unauthorized("Authentication has failed.")
    session['oauth'] = resp
    if 'googleapis.com' in oauth_provider.base_url:
        me = oauth_provider.get('userinfo')
        session['user'] = me.data.get('email')
    elif 'investigativedashboard.org' in oauth_provider.base_url:
        me = oauth_provider.get('api/2/accounts/profile/')
        session['user'] = me.data.get('email')
    else:
        return Unauthorized('Unknown OAuth provider: %r' %
                            oauth_provider.base_url)
    log.info("Logged in: %s", session['user'])
    return redirect(session.pop('next_url', '/'))
