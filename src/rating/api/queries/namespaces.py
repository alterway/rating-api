import random
import string

from flask import abort, jsonify, make_response

from kubernetes import client
from kubernetes.client.rest import ApiException

from rating.api.check import date_checker_start_end
from rating.api.db import db


import sqlalchemy as sa


def create_tenant_namespace(tenant, quantity):
    api = client.CoreV1Api()
    labels = {
        'tenant': tenant
    }
    for number in range(int(quantity)):
        rd = ''.join(
            [random.choice(string.ascii_letters + string.digits) for n in range(8)])
        name = f'{tenant}-{number}-{rd}'.lower()
        meta = client.V1ObjectMeta(labels=labels, name=name)
        namespace = client.V1Namespace(metadata=meta)
        try:
            api.create_namespace(namespace)
        except ApiException as exc:
            if exc.status == 403:
                abort(make_response(jsonify(msg=exc.message)), exc.status)
            raise exc


def delete_namespace(namespace):
    try:
        client.CoreV1Api().delete_namespace(namespace)
    except ApiException as exc:
        if exc.status == 404:
            abort(make_response(jsonify(msg=exc.message)), exc.status)
        raise exc


def modify_namespace(tenant, namespace):
    api = client.CoreV1Api()
    labels = {
        'tenant': tenant
    }
    meta = client.V1ObjectMeta(labels=labels, name=namespace)
    ns = client.V1Namespace(metadata=meta)
    try:
        api.patch_namespace(namespace, ns)
    except ApiException as exc:
        if exc.status == 404:
            abort(make_response(jsonify(msg=exc.message)), exc.status)
        raise exc


def update_namespace(namespace, tenant_id):
    qry = sa.text("""
        INSERT INTO namespaces(namespace, tenant_id)
        VALUES (:namespace, :tenant)
        ON CONFLICT ON CONSTRAINT namespaces_pkey
        DO NOTHING
    """)

    params = {
        'namespace': namespace,
        'tenant': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return res.rowcount


def get_namespaces(tenant_id):
    # XXX tables namespace_status and namespaces are redundant? can do with one?
    qry = sa.text("""
        SELECT namespace
        FROM namespace_status
        WHERE namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        ORDER BY namespace
    """)
    params = {
        'tenant_id': tenant_id
    }
    return [dict(row) for row in db.engine.execute(qry.params(**params))]


@date_checker_start_end
def get_namespace_rating(namespace,
                         start,
                         end,
                         tenant_id):
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                metric
        FROM frames
        WHERE namespace = :namespace
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """)

    params = {
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_namespaces_rating(start,
                          end,
                          tenant_id):
    # XXX should fail because missing tenant_id
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, namespace
    """)

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_namespace_total_rating(namespace,
                               start,
                               end,
                               tenant_id):
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   namespace,
                                   node,
                                   pod
        FROM frames
        WHERE namespace = :namespace
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY namespace, node, pod
        ORDER BY node, pod
    """)

    params = {
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


def get_namespace_tenant(namespace):
    qry = sa.text("""
        SELECT tenant_id
        FROM namespaces
        WHERE namespace = :namespace
        ORDER BY tenant_id
    """)
    return [dict(row) for row in db.engine.execute(qry.params(namespace=namespace))]


@date_checker_start_end
def get_namespaces_total_rating(start,
                                end,
                                tenant_id):
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                                    namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY namespace
        ORDER BY namespace
    """)

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_namespace_pods(namespace,
                       start,
                       end,
                       tenant_id):
    qry = sa.text("""
        SELECT  namespace,
                pod
        FROM frames
        WHERE namespace = :namespace
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY namespace, pod
        ORDER BY pod
    """)

    params = {
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_namespace_nodes(namespace,
                        start,
                        end,
                        tenant_id):
    qry = sa.text("""
        SELECT  namespace,
                node
        FROM frames
        WHERE namespace = :namespace
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY namespace, node
        ORDER BY node
    """)

    params = {
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_namespace_nodes_pods(namespace,
                             start,
                             end,
                             tenant_id):
    qry = sa.text("""
        SELECT  pod,
                node
        FROM frames
        WHERE namespace = :namespace
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN
        (SELECT namespace FROM namespaces WHERE tenant_id = :tenant_id)
        GROUP BY node, pod
        ORDER BY node, pod
    """)

    params = {
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_namespace_metric_rating(namespace,
                                metric,
                                start,
                                end):
    qry = sa.text("""
        SELECT frame_begin,
               frame_end,
               metric,
               frame_price,
               namespace,
               node,
               pod
        FROM frames
        WHERE namespace = :namespace
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        ORDER BY frame_begin, metric
    """)

    params = {
        'namespace': namespace,
        'metric': metric,
        'start': start,
        'end': end
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]


@date_checker_start_end
def get_namespace_metric_total_rating(namespace,
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
        WHERE namespace = :namespace
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        GROUP BY metric, namespace, node, pod
        ORDER BY node, pod, metric
    """)

    params = {
        'namespace': namespace,
        'metric': metric,
        'start': start,
        'end': end
    }

    res = db.engine.execute(qry.params(**params))
    return [dict(row) for row in res]
