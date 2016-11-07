from memorious.core import create_app, celery as app  # noqa

from memorious.loader import load_records  # noqa

flask_app = create_app()
flask_app.app_context().push()
