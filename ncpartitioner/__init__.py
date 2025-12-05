from flask import Flask


def create_app(config=None):
    app = Flask(__name__)
    app.config.from_object(config)

    with app.app_context():
        from .routes import partition

        app.register_blueprint(partition)

    return app
