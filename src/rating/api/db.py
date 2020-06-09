from flask_sqlalchemy import SQLAlchemy

from pyhive import sqlalchemy_presto

from rating.api import config

import sqlalchemy
from sqlalchemy import create_engine


def setup_database(app):
    db.init_app(app)


def presto_engine():
    sqlalchemy.dialects.presto = sqlalchemy_presto
    presto_database_uri = config.envvar('PRESTO_DATABASE_URI')
    return create_engine(presto_database_uri)


db = SQLAlchemy()
presto_db = presto_engine()
