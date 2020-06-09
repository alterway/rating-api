from rating.api.check import date_checker_start_end
from rating.api.db import db

import sqlalchemy as sa


def get_pods(start,
             end,
             tenant_id):
    qry = sa.text("""
        SELECT pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY pod
        ORDER BY pod
    """)

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pods_rating(start,
                    end,
                    tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                frame_end,
                frame_price,
                metric,
                namespace,
                node,
                pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        ORDER BY frame_begin, metric
    """)

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pods_total_rating(start,
                          end,
                          tenant_id):
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                                    pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        GROUP BY pod
        ORDER BY pod
    """)

    params = {
        'start': start,
        'end': end
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pod_total_rating(pod,
                         start,
                         end,
                         tenant_id):
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   namespace,
                                   node,
                                   pod
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY namespace, node, pod
        ORDER BY namespace, node, pod
    """)

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pod_namespace(pod,
                      start,
                      end,
                      tenant_id):
    qry = sa.text("""
        SELECT  pod,
                namespace
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY pod, namespace
    """)

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pod_node(pod,
                 start,
                 end,
                 tenant_id):
    qry = sa.text("""
        SELECT  pod,
                node
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY pod, node
        ORDER BY pod
    """)

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pod_rating(pod,
                   start,
                   end,
                   tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                metric,
                pod
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin, metric, pod
        ORDER BY frame_begin, metric, pod
    """)

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pod_metric_rating(pod,
                          metric,
                          start,
                          end):
    qry = sa.text("""
        SELECT  frame_begin,
                frame_end,
                metric,
                frame_price,
                namespace,
                node,
                pod
        FROM frames
        WHERE pod = :pod
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        ORDER BY frame_begin, metric
    """)

    params = {
        'pod': pod,
        'metric': metric,
        'start': start,
        'end': end
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_pod_metric_total_rating(pod,
                                metric,
                                start,
                                end):
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   metric,
                                   namespace,
                                   node,
                                   pod
        FROM frames
        WHERE pod = :pod
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        GROUP BY metric, namespace, node, pod
        ORDER BY metric
    """)

    params = {
        'pod': pod,
        'metric': metric,
        'start': start,
        'end': end
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


def get_pod_lifetime(pod, tenant_id):
    qry = sa.text("""
        SELECT  min(frame_begin) as start,
                max(frame_end) as end
        FROM frames
        WHERE pod = :pod
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
    """)
    params = {
        'pod': pod,
        'tenant_id': tenant_id
    }
    return [dict(row) for row in db.engine.execute(qry.params(**params))]
