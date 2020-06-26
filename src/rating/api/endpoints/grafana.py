import datetime

from flask import Blueprint, current_app, jsonify, make_response, request, url_for

import requests

import werkzeug


grafana_routes = Blueprint('grafana', __name__)


def unallowed_routes():
    return [
        '/',
        '/search',
        '/query',
        '/annotations',
        '/alive',
        '/rules_metrics',
        '/rating/configs/list',
        '/rating/configs/<timestamp>',
        '/presto/<table>/columns',
        '/presto/<table>/frames',
        '/signup',
        '/login',
        '/logout',
        '/current',
        '/tenant',
        '/tenants',
        '/static/<path:filename>'
    ]


@grafana_routes.route('/')
def grafana_routes_handshake():
    return make_response('Succesful handshake', 200)


@grafana_routes.route('/search', methods=['GET', 'POST'])
def search_grafana_routes():
    target = request.get_json()['target']
    # TODO sort queries according to type
    # find a reliable way to sort without hardcoding
    links = []
    for rule in current_app.url_map.iter_rules():
        string_rule = str(rule)
        if string_rule in unallowed_routes():
            continue
        elif 'GET' in rule.methods and target in string_rule:
            endpoint = {
                'text': string_rule,
                'value': string_rule
            }
            links.append(endpoint)
    return jsonify(links)


@grafana_routes.route('/query', methods=['GET', 'POST'])
def query_metrics():
    req = request.get_json()

    payload = []
    time_range = {
        'start': req['range']['from'].replace('T', ' '),
        'end': req['range']['to'].replace('T', ' ')
    }

    for target in req['targets']:
        if not target.get('target'):
            continue

        route = find_matching_route(target['target'])
        if not route:
            continue

        params = {**time_range, **strip_unused_keys(route, target.get('data') or {})}
        try:
            url = url_for(route, **params)
        except werkzeug.routing.BuildError as exc:
            return make_response(str(exc), 422)
        results = requests.get(
            f'http://localhost:5012{url}',
            cookies=request.cookies).json()['results']
        if not results:
            continue

        responses = {
            'table': format_table_response,
            'timeseries': format_timeserie_response
        }[target['type']](results, additionnal=params)

        for response in responses:
            payload.append(response)
    if payload:
        return make_response(jsonify(payload), 200)
    return make_response('No metric found', 404)


def strip_unused_keys(route, params):
    for key in list(params.keys()):
        if key not in route:
            del params[key]
    return params


def match_key_type(key):
    try:
        return {
            'frame_begin': 'time',
            'frame_end': 'time',
            'metric': 'string',
            'tenant': 'string',
            'pod': 'string',
            'node': 'string',
            'namespace': 'string'
        }[key]
    except KeyError:
        return 'number'


def format_table_response(content, additionnal={}):
    columns = []
    for key in content[0].keys():
        columns.append({
            'text': key,
            'type': match_key_type(key)
        })

    return [{
        'columns': columns,
        'rows': [list(metric.values()) for metric in content],
        'type': 'table'
    }]


def to_timestamp(epoch):
    return int(
        datetime.datetime.strptime(epoch, '%a, %d %b %Y %H:%M:%S GMT').timestamp() * 1000)


def format_timeserie_response(content, additionnal={}):
    data = {}
    response = []
    for row in content:
        if 'frame_begin' not in row.keys():
            return response

        sort = ''
        for index in ['node', 'namespace', 'pod', 'metric']:
            if index not in row or index in additionnal:
                continue
            if sort:
                sort += '_'
            sort += row[index]

        if sort not in data:
            data[sort] = []

        data[sort].append(
            [
                row.get('frame_price', 1),
                to_timestamp(row['frame_begin'])
            ])

    for key in data.keys():
        response.append({
            'target': key,
            'datapoints': data[key]
        })
    return response


def find_matching_route(target):
    for rule in current_app.url_map.iter_rules():
        if str(rule) == target:
            return rule.endpoint
    return None
