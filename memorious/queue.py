from memorious.core import create_app, celery as app  # noqa

flask_app = create_app()
flask_app.app_context().push()
