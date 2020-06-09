from flask import Blueprint, request

from rating.api.check import request_params
from rating.api.queries import nodes as query

from .auth import with_session


nodes_routes = Blueprint('nodes', __name__)


@nodes_routes.route('/nodes')
@with_session
def nodes(tenant):
    rows = query.get_nodes(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/rating')
@with_session
def nodes_rating(tenant):
    config = request_params(request.args)
    rows = query.get_nodes_rating(start=config['start'],
                                  end=config['end'],
                                  tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/total_rating')
@with_session
def nodes_total_rating(tenant):
    config = request_params(request.args)
    rows = query.get_nodes_total_rating(start=config['start'],
                                        end=config['end'],
                                        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/rating')
@with_session
def node_rating(node, tenant):
    config = request_params(request.args)
    rows = query.get_node_rating(node=node,
                                 start=config['start'],
                                 end=config['end'],
                                 tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/namespaces')
@with_session
def node_namespaces(node, tenant):
    config = request_params(request.args)
    rows = query.get_node_namespaces(node=node,
                                     start=config['start'],
                                     end=config['end'],
                                     tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/pods')
@with_session
def node_pods(node, tenant):
    config = request_params(request.args)
    rows = query.get_node_pods(node=node,
                               start=config['start'],
                               end=config['end'],
                               tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/total_rating')
@with_session
def node_total_rating(node, tenant):
    config = request_params(request.args)
    rows = query.get_node_total_rating(node=node,
                                       start=config['start'],
                                       end=config['end'],
                                       tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/namespaces/rating')
@with_session
def node_namespaces_rating(node, tenant):
    config = request_params(request.args)
    rows = query.get_node_namespaces_rating(node=node,
                                            start=config['start'],
                                            end=config['end'],
                                            tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/namespaces/<namespace>/rating')
@with_session
def node_namespace_rating(node, namespace, tenant):
    config = request_params(request.args)
    rows = query.get_node_namespace_rating(node=node,
                                           namespace=namespace,
                                           start=config['start'],
                                           end=config['end'],
                                           tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/namespaces/<namespace>/total_rating')
@with_session
def node_namespace_total_rating(node, namespace, tenant):
    config = request_params(request.args)
    rows = query.get_node_namespace_total_rating(node=node,
                                                 namespace=namespace,
                                                 start=config['start'],
                                                 end=config['end'],
                                                 tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/metrics/<metric>/rating')
@with_session
def node_metric_rating(node, metric, tenant):
    config = request_params(request.args)
    rows = query.get_node_metric_rating(node=node,
                                        metric=metric,
                                        start=config['start'],
                                        end=config['end'],
                                        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/metrics/<metric>/total_rating')
@with_session
def node_metric_total_rating(node, metric, tenant):
    config = request_params(request.args)
    rows = query.get_node_metric_total_rating(node=node,
                                              metric=metric,
                                              start=config['start'],
                                              end=config['end'],
                                              tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
