from rating.api.db import db

from sqlalchemy import text


def new_tenant(tenant, password):
    qry = text("""
        INSERT INTO users (tenant_id, password)
        VALUES (:tenant, :password)
    """)
    params = {
        'tenant': tenant,
        'password': password
    }
    res = db.engine.execute(qry.params(**params))
    return res.rowcount


def get_tenant(tenant):
    qry = text("""
        SELECT *
        FROM namespaces
        WHERE tenant_id = :tenant
    """)
    res = db.engine.execute(qry.params(tenant=tenant))
    return [dict(row) for row in res]


def get_tenant_id(tenant):
    qry = text("""
        SELECT *
        FROM users
        WHERE tenant_id = :tenant
    """)
    res = db.engine.execute(qry.params(tenant=tenant))
    return [dict(row) for row in res]


def link_namespace(tenant, namespace):
    qry = text("""
        UPDATE namespaces
        SET tenant_id = :tenant
        WHERE namespace = :namespace
    """)
    params = {
        'tenant': tenant,
        'namespace': namespace
    }
    res = db.engine.execute(qry.params(**params))
    return res.rowcount


def unlink_namespace(namespace):
    qry = text("""
        UPDATE namespaces
        SET tenant_id = 'default'
        WHERE namespace = :namespace
    """)
    res = db.engine.execute(qry.params(namespace=namespace))
    return res.rowcount


def delete_tenant(tenant):
    qry = text("""
        DELETE FROM namespaces
        WHERE tenant_id = :tenant
    """)
    res = db.engine.execute(qry.params(tenant=tenant))
    return res.rowcount


def get_tenants():
    qry = text("""
        SELECT tenant_id
        FROM namespaces
        GROUP BY tenant_id
    """)
    return [dict(row) for row in db.engine.execute(qry)]


def get_tenant_namespaces(tenant):
    qry = text("""
        SELECT namespace
        from namespaces
        WHERE tenant_id = :tenant
    """)
    return [row[0] for row in db.engine.execute(qry.params(tenant=tenant))]
