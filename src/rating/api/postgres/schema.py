
import logging
import re
from pathlib import Path
from textwrap import dedent

import pkg_resources


class DatabaseError(Exception):
    pass


# This schema upgrade mechanism is simplistic and doesn't work well with code merges.
# Alembic would be better though more complex.


schema_version_table = dedent("""
    CREATE TABLE IF NOT EXISTS schema_version (
        singleton INTEGER DEFAULT 0 PRIMARY KEY CHECK (singleton=0),
        version INTEGER DEFAULT 0
    );
    INSERT INTO schema_version SELECT 0, 0 WHERE NOT EXISTS (
        SELECT * FROM schema_version
    );
""")


def fetch_scripts():
    sql_dir = Path(pkg_resources.resource_filename('rating.api', 'postgres'),
                   'scripts')
    renum = re.compile('\\d+')

    return {
        int(renum.match(script.name).group()): script
        for script in sql_dir.glob('*.sql')
    }


def db_update(engine):
    scripts = fetch_scripts()
    required_version = max(scripts)

    conn = engine.connect()
    with conn.begin():
        conn.execute(schema_version_table)
        cur_version = conn.execute('SELECT version FROM schema_version').scalar()

        if cur_version > required_version:
            raise DatabaseError(f'Database schema is at version {cur_version}, which is '
                                'unsupported. Please upgrade the application.') from None

        if cur_version == required_version:
            logging.info('No schema update required.')

        for script_version, script in sorted(scripts.items()):
            if script_version <= cur_version:
                continue

            logging.info(f'Applying {script}')

            with script.open(encoding='utf8') as fin:
                query = fin.read()
                if query.strip():
                    conn.execute(query)

            conn.execute('UPDATE schema_version SET version=%s', [script_version])
        logging.info('Schema update complete.')