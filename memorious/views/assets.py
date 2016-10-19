from flask_assets import Bundle

from memorious.core import assets

css_assets = Bundle(
    'style/main.scss',
    depends=['**/*.scss'],
    filters='scss,cssutils',
    output='assets/style.css'
)


def compile_assets(app):
    assets._named_bundles = {}
    assets.register('css', css_assets)
