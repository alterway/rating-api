from flask import Blueprint, request

from flask_json import as_json

from rating.api.check import assert_url_params, request_params
from rating.api.queries import frames as query
from rating.api.secret import require_admin
from rating.api.write_frames import write_rated_frames

frames_routes = Blueprint('frames', __name__)


@frames_routes.route('/presto/<table>/columns')
@as_json
@require_admin
@assert_url_params
def table_columns(table):
    rows = query.get_table_columns(table)
    return {
        'total': len(rows),
        'results': rows
    }


@frames_routes.route('/presto/<table>/frames')
@as_json
@require_admin
@assert_url_params
def unrated_frames(table):
    config = request_params(request.args)
    rows = query.get_unrated_frames(
        table=table,
        column=config['column'],
        labels=config['labels'],
        start=config['start'],
        end=config['end'])
    return {
        'total': len(rows),
        'results': rows
    }


@frames_routes.route('/rated/frames/add', methods=['POST'])
@as_json
@require_admin
def rated_frames_add():
    received = request.get_json()
    write_rated_frames(frames=received['body']['rated_frames'])
    query.update_rated_namespaces(
        namespaces=received['body']['rated_namespaces'],
        last_insert=received['body']['last_insert'])
    query.update_rated_metrics(
        metric=received['body']['metric'],
        report_name=received['body']['report_name'],
        last_insert=received['body']['last_insert'])
    query.update_rated_metrics_object(
        metric=received['body']['metric'],
        last_insert=received['body']['last_insert'])
    return {
        'total': 1,
        'results': 1
    }


@frames_routes.route('/rated/frames/delete', methods=['POST'])
@as_json
@require_admin
def rated_frames_delete():
    received = request.get_json()
    rows = query.delete_rated_frames(metric=received['body']['metric'])
    return {
        'total': query.clear_rated_metrics(metric=received['body']['metric']),
        'results': rows
    }
