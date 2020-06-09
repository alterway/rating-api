from kubernetes import client, config
from kubernetes.client.rest import ApiException

from rating.api.config import envvar
from rating.api.db import db, presto_db

import sqlalchemy as sa


def get_table_columns(table):
    qry = sa.text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table
        ORDER BY ordinal_position
    """)

    params = {
        'table': table
    }

    return [dict(row) for row in presto_db.execute(qry.params(**params))]


def get_unrated_frames(table,
                       column,
                       labels,
                       start,
                       end):
    with_table = f"""
        SELECT period_start,
               period_end,
               namespace,
               node,
               pod,
               {column} {labels}
        FROM {table}
        WHERE period_end >= TIMESTAMP :start
            AND
              period_end < TIMESTAMP :end
    """
    qry = sa.text(with_table)

    params = {
        'start': start,
        'end': end
    }
    return [dict(row) for row in presto_db.execute(qry.params(**params))]


def update_rated_metrics(metric, report_name, last_insert):
    qry = sa.text("""
        INSERT INTO frame_status(last_insert, report_name, metric)
        VALUES (:last_insert, :report_name , :metric)
        ON CONFLICT (report_name, metric)
        DO UPDATE SET last_insert = :last_insert
    """)

    params = {
        'last_insert': last_insert,
        'report_name': report_name,
        'metric': metric
    }
    res = db.engine.execute(qry.params(**params))
    return res.rowcount


def update_rated_metrics_object(metric, last_insert):
    config.load_incluster_config()
    rated_metric = f'rated-{metric.replace("_", "-")}'
    rated_namespace = envvar('RATING_NAMESPACE')
    custom_api = client.CustomObjectsApi()
    body = {
        'apiVersion': 'rating.alterway.fr/v1',
        'kind': 'RatedMetric',
        'metadata': {
            'namespace': rated_namespace,
            'name': rated_metric,
        },
        'spec': {
            'metric': metric,
            'date': last_insert
        }
    }
    try:
        custom_api.create_namespaced_custom_object(group='rating.alterway.fr',
                                                   version='v1',
                                                   namespace=rated_namespace,
                                                   plural='ratedmetrics',
                                                   body=body)
    except ApiException as exc:
        if exc.status != 409:
            raise exc
        custom_api.patch_namespaced_custom_object(group='rating.alterway.fr',
                                                  version='v1',
                                                  namespace=rated_namespace,
                                                  plural='ratedmetrics',
                                                  name=rated_metric,
                                                  body=body)


def update_rated_namespaces(namespaces, last_insert):
    for namespace in namespaces:
        qry = sa.text("""
            INSERT INTO namespace_status (namespace, last_update)
            VALUES (:namespace, :last_update)
            ON CONFLICT ON CONSTRAINT namespace_status_pkey
            DO UPDATE SET last_update = EXCLUDED.last_update
        """)
        params = {
            'namespace': namespace,
            'last_update': last_insert
        }
        db.engine.execute(qry.params(**params))
    return len(namespaces)


def clear_rated_metrics(metric):
    qry = sa.text("""
        DELETE FROM frame_status
        WHERE metric = :metric
    """)
    params = {
        'metric': metric
    }
    res = db.engine.execute(qry.params(**params))
    return res.rowcount


def delete_rated_frames(metric):
    qry = sa.text("""
        DELETE FROM frames
        WHERE metric = :metric
    """)
    params = {
        'metric': metric
    }
    res = db.engine.execute(qry.params(**params))
    return res.rowcount
