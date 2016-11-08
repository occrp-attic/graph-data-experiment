from flask_assets import Bundle

from memorious.core import assets

css_assets = Bundle(
    'style/main.scss',
    depends=['**/*.scss'],
    filters='scss,cssutils',
    output='assets/style.css'
)

js_assets = Bundle(
    'js/util.js',
    output='assets/app.js'
)


def compile_assets(app):
    assets._named_bundles = {}
    assets.register('css', css_assets)
    assets.register('js', js_assets)
