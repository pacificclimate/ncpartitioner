from flask import Flask
import logging

def create_app(config=None):
    app = Flask(__name__)
    app.config.from_object(config)
    app.logger.setLevel(logging.INFO)

    with app.app_context():
        from .routes import partition

        app.register_blueprint(partition)

    return app
