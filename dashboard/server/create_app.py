
from views.app import api_bp
from flask import Flask, g, jsonify
from werkzeug.serving import run_simple
from flask_sqlalchemy import SQLAlchemy
from config import config_setting
from flask_jwt import JWT, jwt_required, current_identity
from datetime import timedelta
from security import authenticate, identity
from user import  User ,UserRegister
def page_not_found(e):
    """Custom error handling for 404"""
    return jsonify({"error": "page not found"})


def create_app(testing: bool = True):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_setting['config'])
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['JWT_AUTH_URL_RULE'] = '/login'
    app.config['JWT_EXPIRATION_DELTA'] = timedelta(seconds=1800)

    jwt = JWT(app, authenticate, identity)
    db = SQLAlchemy(app)

    app.register_blueprint(api_bp)
    app.register_error_handler(404, page_not_found)

    @app.before_request
    def before_request() -> None:
        g.testing = app.testing

    app.app_context().push()  # this is needed for application global context

    return app


application = create_app()

#for development
if __name__ == '__main__':
    run_simple('localhost', 8000, application,
               use_reloader=True, use_debugger=True, use_evalex=True)