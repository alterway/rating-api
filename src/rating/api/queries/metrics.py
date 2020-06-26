from rating.api.check import date_checker_start_end
from rating.api.db import db

import sqlalchemy as sa


def get_tenants():
    qry = sa.text("""
        SELECT tenant_id
        FROM namespaces
        GROUP BY tenant_id
        ORDER BY tenant_id
    """)
    return [dict(row) for row in db.engine.execute(qry)]


def get_tenant_namespace(tenant):
    qry = sa.text("""
        SELECT namespace
        FROM namespaces
        WHERE tenant_id = :tenant
        ORDER BY namespace
    """)
    return [dict(row) for row in db.engine.execute(qry.params(tenant=tenant))]


@date_checker_start_end
def get_metric_rating(metric,
                      start,
                      end,
                      tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as price
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin
        ORDER BY frame_begin
    """)

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_metric_total_rating(metric,
                            start,
                            end,
                            tenant_id):
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   metric
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY metric
    """)

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


def get_metrics(tenant_id):
    qry = sa.text("""
        SELECT metric
        FROM frame_status
        ORDER BY metric
    """)

    return [dict(row) for row in db.engine.execute(qry)]


def get_metric_report(metric, tenant_id):
    qry = sa.text("""
        SELECT report_name
        FROM frame_status
        WHERE metric = :metric
    """)

    params = {
        'metric': metric
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


def get_last_rated_date(metric, tenant_id):
    qry = sa.text("""
        SELECT last_insert
        FROM frame_status
        WHERE metric = :metric
    """)

    params = {
        'metric': metric
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


def get_last_rated_metrics(tenant_id):
    qry = sa.text("""
        SELECT last_insert,
               report_name,
               metric
        FROM frame_status
    """)

    return [dict(row) for row in db.engine.execute(qry)]


def get_report_metric(report, tenant_id):
    qry = sa.text("""
        SELECT metric
        FROM frame_status
        WHERE report_name = :report
    """)

    params = {
        'report': report
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


def get_last_rated_reports(report, tenant_id):
    qry = sa.text("""
        SELECT last_insert
        FROM frame_status
        WHERE report_name = :report
    """)

    params = {
        'report': report
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]
