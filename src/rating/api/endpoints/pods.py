from flask import Blueprint, request

from rating.api.check import request_params
from rating.api.queries import pods as query

from .auth import with_session


pods_routes = Blueprint('pods', __name__)


@pods_routes.route('/pods')
@with_session
def pods(tenant):
    config = request_params(request.args)
    rows = query.get_pods(start=config['start'],
                          end=config['end'],
                          tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/lifetime')
@with_session
def pod_lifetime(pod, tenant):
    rows = query.get_pod_lifetime(pod=pod,
                                  tenant_id=tenant)
    # XXX can return 404 if you want, if rows is empty
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/rating')
@with_session
def pods_rating(tenant):
    config = request_params(request.args)
    rows = query.get_pods_rating(start=config['start'],
                                 end=config['end'],
                                 tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/total_rating')
@with_session
def pods_total_rating(tenant):
    config = request_params(request.args)
    rows = query.get_pods_total_rating(start=config['start'],
                                       end=config['end'],
                                       tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/rating')
@with_session
def pod_rating(pod, tenant):
    config = request_params(request.args)
    rows = query.get_pod_rating(pod=pod,
                                start=config['start'],
                                end=config['end'],
                                tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/total_rating')
@with_session
def pod_total_rating(pod, tenant):
    config = request_params(request.args)
    rows = query.get_pod_total_rating(pod=pod,
                                      start=config['start'],
                                      end=config['end'],
                                      tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/namespace')
@with_session
def pod_namespace(pod, tenant):
    config = request_params(request.args)
    rows = query.get_pod_namespace(pod=pod,
                                   start=config['start'],
                                   end=config['end'],
                                   tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/node')
@with_session
def pod_node(pod, tenant):
    config = request_params(request.args)
    rows = query.get_pod_node(pod=pod,
                              start=config['start'],
                              end=config['end'],
                              tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/metrics/<metric>/rating')
@with_session
def pod_metric_rating(pod, metric, tenant):
    config = request_params(request.args)
    rows = query.get_pod_metric_rating(pod=pod,
                                       metric=metric,
                                       start=config['start'],
                                       end=config['end'],
                                       tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/metrics/<metric>/total_rating')
@with_session
def pod_metric_total_rating(pod, metric, tenant):
    config = request_params(request.args)
    rows = query.get_pod_metric_total_rating(pod=pod,
                                             metric=metric,
                                             start=config['start'],
                                             end=config['end'],
                                             tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
