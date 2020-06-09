from flask import Blueprint, request

from flask_json import as_json

from rating.api.check import request_params
from rating.api.queries import namespaces as query
from rating.api.secret import require_admin

from .auth import with_session


namespaces_routes = Blueprint('namespaces', __name__)


@namespaces_routes.route('/namespaces')
@with_session
def namespaces(tenant):
    rows = query.get_namespaces(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/tenant', methods=['POST'])
@as_json
@require_admin
def update_namespace_tenant():
    received = request.get_json()
    rows = query.update_namespace(namespace=received['body']['namespace'],
                                  tenant_id=received['body']['tenant_id'])
    return {
        'total': 1,
        'results': rows
    }


@namespaces_routes.route('/namespaces/rating')
@with_session
def namespaces_rating(tenant):
    config = request_params(request.args)
    rows = query.get_namespaces_rating(
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/total_rating')
@with_session
def namespaces_total_rating(tenant):
    config = request_params(request.args)
    rows = query.get_namespaces_total_rating(
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


# @namespaces_routes.route('/namespaces/<namespace>/tenant')
# @with_session
# def namespace_tenant(namespace):
#     rows = query.get_namespace_tenant(namespace=namespace)
#     return {
#         'total': len(rows),
#         'results': rows
#     }


@namespaces_routes.route('/namespaces/<namespace>/total_rating')
@with_session
def namespace_total_rating(namespace, tenant):
    config = request_params(request.args)
    rows = query.get_namespace_total_rating(
        namespace=namespace,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/rating')
@with_session
def namespace_rating(namespace, tenant):
    config = request_params(request.args)
    rows = query.get_namespace_rating(
        namespace=namespace,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/pods')
@with_session
def namespace_pods(namespace, tenant):
    config = request_params(request.args)
    rows = query.get_namespace_pods(
        namespace=namespace,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/nodes')
@with_session
def namespace_nodes(namespace, tenant):
    config = request_params(request.args)
    rows = query.get_namespace_nodes(
        namespace=namespace,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/nodes/pods')
@with_session
def namespace_nodes_pods(namespace, tenant):
    config = request_params(request.args)
    rows = query.get_namespace_nodes_pods(
        namespace=namespace,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/metrics/<metric>/rating')
@with_session
def namespace_metric_rating(namespace, metric, tenant):
    config = request_params(request.args)
    rows = query.get_namespace_metric_rating(
        namespace=namespace,
        metric=metric,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/metrics/<metric>/total_rating')
@with_session
def namespace_metric_total_rating(namespace, metric, tenant):
    config = request_params(request.args)
    rows = query.get_namespace_metric_total_rating(
        namespace=namespace,
        metric=metric,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
