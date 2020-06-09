from flask import Flask

from flask_cors import CORS

from rating.api import db
from rating.api.endpoints.auth import auth_routes
from rating.api.endpoints.configs import configs_routes
from rating.api.endpoints.frames import frames_routes
from rating.api.endpoints.metrics import metrics_routes
from rating.api.endpoints.namespaces import namespaces_routes
from rating.api.endpoints.nodes import nodes_routes
from rating.api.endpoints.pods import pods_routes
from rating.api.endpoints.tenants import tenants_routes
from rating.api.postgres import engine
from rating.api.secret import register_admin_key


def create_app():
    app = Flask(__name__)
    CORS(app, supports_credentials=True, origins='http://localhost:8080')
    app.secret_key = register_admin_key()
    app.config.from_object('src.rating.api.config.Config')
    app.register_blueprint(frames_routes)
    app.register_blueprint(metrics_routes)
    app.register_blueprint(configs_routes)
    app.register_blueprint(namespaces_routes)
    app.register_blueprint(pods_routes)
    app.register_blueprint(nodes_routes)
    app.register_blueprint(auth_routes)
    app.register_blueprint(tenants_routes)
    db.setup_database(app)
    engine.update_postgres_schema()
    return app
