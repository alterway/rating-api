from flask import Blueprint, make_response, request

from rating.api import config
from rating.api.check import request_params
from rating.api.queries import metrics as query

from .auth import with_session


metrics_routes = Blueprint('metrics', __name__)


@metrics_routes.route('/alive')
def ping():
    return "I'm alive!"


# @metrics_routes.route('/tenants')
# @with_session
# def tenants():
#     rows = query.get_tenants()
#     return {
#         'total': len(rows),
#         'results': rows
#     }


# @metrics_routes.route('/tenants/<tenant>/namespace')
# @with_session
# def tenant_namespace(tenant):
#     rows = query.get_tenant_namespace(tenant=tenant)
#     return {
#         'total': len(rows),
#         'results': rows
#     }


@metrics_routes.route('/metrics')
@with_session
def metrics(tenant):
    rows = query.get_metrics(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/rating')
@with_session
def metric_rating(metric, tenant):
    config = request_params(request.args)
    rows = query.get_metric_rating(metric=metric,
                                   start=config['start'],
                                   end=config['end'],
                                   tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/total_rating')
@with_session
def metric_total_rating(metric, tenant):
    config = request_params(request.args)
    rows = query.get_metric_total_rating(metric=metric,
                                         start=config['start'],
                                         end=config['end'],
                                         tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/report')
@with_session
def metric_report(metric, tenant):
    rows = query.get_metric_report(metric=metric,
                                   tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/last_rated')
@with_session
def metric_last_rated_date(metric, tenant):
    rows = query.get_last_rated_date(metric=metric,
                                     tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/last_rated')
@with_session
def metrics_last_rated_date(tenant):
    rows = query.get_last_rated_metrics(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/reports/<report>/metric')
@with_session
def report_metric(report, tenant):
    rows = query.get_report_metric(report=report,
                                   tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/reports/<report_name>/last_rated')
@with_session
def report_last_rated(report_name, tenant):
    rows = query.get_last_rated_reports(report_name=report_name,
                                        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/rules_metrics')
def config_lookup_export():
    rows = config.generate_rules_export()
    return make_response('\n'.join(rows) + '\n# EOF\n', 200)
