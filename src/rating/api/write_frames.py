import csv
import logging
import tempfile
from datetime import datetime as dt

import postgres_copy

from rating.api.db import db


class TableWrap():
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name


def write_to_csv(file, frames):
    for frame in frames:
        for it in [0, 1]:
            try:
                frame[it] = dt.strptime(frame[it], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                frame[it] = dt.strptime(frame[it].split('.')[0], '%Y-%m-%d %H:%M:%S')
    writer = csv.writer(file, delimiter='\t')
    writer.writerows(frames)
    file.flush()
    file.seek(0)


def write_rated_frames(frames):
    merging_frames = """
        INSERT INTO frames
        SELECT DISTINCT *
        FROM frames_copy
        ON CONFLICT ON CONSTRAINT frames_pkey DO NOTHING
    """
    res = db.engine.execute('TRUNCATE frames_copy')
    connection = db.engine.raw_connection()
    with tempfile.NamedTemporaryFile(mode='w+',
                                     encoding='utf-8',
                                     delete=True) as f:
        write_to_csv(f, frames)
        logging.info(f'wrote to temp file {f.name}')
        with connection.cursor() as cursor:
            logging.info(f'copying from {f.name} to frames_copy..')
            postgres_copy.copy_from(
                f,
                TableWrap(schema='public', name='frames_copy'),
                connection)
            logging.info('merging frames_copy into frames..')
            cursor.execute(merging_frames)
            connection.commit()

    connection.close()
    logging.info('done!')
    return res
