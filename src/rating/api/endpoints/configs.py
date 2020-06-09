from flask import Blueprint, abort, jsonify, make_response, request

from flask_json import as_json

from rating.api import config
from rating.api import schema
from rating.api.secret import require_admin

from .auth import authenticated_user


configs_routes = Blueprint('configs', __name__)


@configs_routes.route('/rating/configs')
@as_json
def rating_configs_all():
    user = authenticated_user(request)
    rows = config.retrieve_configurations(tenant_id=user)
    return {
        'total': len(rows),
        'results': rows
    }


@configs_routes.route('/rating/configs/list')
@as_json
def rating_configs_list():
    user = authenticated_user(request)
    rows = config.retrieve_directories(tenant_id=user)
    return {
        'total': len(rows),
        'results': rows
    }


@configs_routes.route('/rating/configs/<timestamp>')
@as_json
def rating_config(timestamp):
    user = authenticated_user(request)
    rows = config.retrieve_config_as_dict(timestamp=timestamp,
                                          tenant_id=user)
    return {
        'total': len(rows),
        'results': rows
    }


@configs_routes.route('/rating/configs/add', methods=['POST'])
@require_admin
@as_json
def new_rating_config():
    received = request.get_json()
    try:
        schema.validate_request_content(received['body'])
        rows = config.create_new_config(content=received['body'])
    except schema.ValidationError as exc:
        abort(make_response(jsonify(message=exc.message), 400))
    else:
        return {
            'total': 1,
            'results': rows
        }


@configs_routes.route('/rating/configs/update', methods=['POST'])
@require_admin
@as_json
def update_rating_config():
    received = request.get_json()
    try:
        schema.validate_request_content(received['body'])
        rows = config.update_config(content=received['body'])
    except config.ConfigurationMissing as exc:
        abort(make_response(jsonify(message=exc.message), 404))
    except schema.ValidationError as err:
        abort(make_response(jsonify(message=err.message), 400))
    else:
        return {
            'total': 1,
            'results': rows
        }


@configs_routes.route('/rating/configs/delete', methods=['POST'])
@require_admin
@as_json
def rating_config_delete():
    received = request.get_json()
    try:
        rows = config.delete_configuration(timestamp=received['body']['timestamp'])
    except config.ConfigurationMissing as exc:
        abort(make_response(jsonify(message=exc.message), 404))
    else:
        return {
            'total': 1,
            'results': rows
        }
