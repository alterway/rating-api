from rating.api.check import date_checker_start_end
from rating.api.db import db

import sqlalchemy as sa


def get_nodes(tenant_id):
    qry = sa.text("""
        SELECT node
        FROM frames
        WHERE namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY node
        ORDER BY node
    """)
    params = {
        'tenant_id': tenant_id
    }
    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_nodes_rating(start,
                     end,
                     tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """)

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_nodes_total_rating(start,
                           end,
                           tenant_id):
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                                    node
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY node
        ORDER BY node
    """)

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_namespaces(node,
                        start,
                        end,
                        tenant_id):
    qry = sa.text("""
        SELECT  node,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND node = :node
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY node, namespace
        ORDER BY namespace
    """)

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_pods(node,
                  start,
                  end,
                  tenant_id):
    qry = sa.text("""
        SELECT  node,
                pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND node = :node
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY node, pod
        ORDER BY pod
    """)

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_namespaces_rating(node,
                               start,
                               end,
                               tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND node = :node
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin, node, namespace
        ORDER BY frame_begin, namespace
    """)

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_namespace_rating(node,
                              namespace,
                              start,
                              end,
                              tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND node = :node
        AND namespace = :namespace
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin, node, namespace
        ORDER BY frame_begin
    """)

    params = {
        'node': node,
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_namespace_total_rating(node,
                                    namespace,
                                    start,
                                    end,
                                    tenant_id):
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                node,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND node = :node
        AND namespace = :namespace
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY node, namespace
        ORDER BY namespace
    """)

    params = {
        'node': node,
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_rating(node,
                    start,
                    end,
                    tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                metric,
                sum(frame_price) as frame_price
        FROM frames
        WHERE node = :node
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """)

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_total_rating(node,
                          start,
                          end,
                          tenant_id):
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   namespace,
                                   node,
                                   pod
        FROM frames
        WHERE node = :node
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY namespace, node, pod
        ORDER BY namespace, pod
    """)

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_metric_rating(node,
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
        WHERE node = :node
        and metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        ORDER BY namespace, pod, metric
    """)

    params = {
        'node': node,
        'metric': metric,
        'start': start,
        'end': end
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_node_metric_total_rating(node,
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
        WHERE node = :node
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        GROUP BY metric, namespace, node, pod
        ORDER BY namespace, pod, metric
    """)

    params = {
        'node': node,
        'metric': metric,
        'start': start,
        'end': end
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]
