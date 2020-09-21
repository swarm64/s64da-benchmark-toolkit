import time

from datetime import timedelta
from dateutil.parser import isoparse
from itertools import cycle
from os import path
from string import Template

import psycopg2
import yaml

from .helpers import Random


TEMPLATE_DIR = path.join('benchmarks', 'htap', 'queries')

QUERY_TEMPLATES = {
     1: Template(open(path.join(TEMPLATE_DIR, '01.sql.template'), 'r').read()),
     2: Template(open(path.join(TEMPLATE_DIR, '02.sql.template'), 'r').read()),
     3: Template(open(path.join(TEMPLATE_DIR, '03.sql.template'), 'r').read()),
     4: Template(open(path.join(TEMPLATE_DIR, '04.sql.template'), 'r').read()),
     5: Template(open(path.join(TEMPLATE_DIR, '05.sql.template'), 'r').read()),
     6: Template(open(path.join(TEMPLATE_DIR, '06.sql.template'), 'r').read()),
     7: Template(open(path.join(TEMPLATE_DIR, '07.sql.template'), 'r').read()),
     8: Template(open(path.join(TEMPLATE_DIR, '08.sql.template'), 'r').read()),
     9: Template(open(path.join(TEMPLATE_DIR, '09.sql.template'), 'r').read()),
    10: Template(open(path.join(TEMPLATE_DIR, '10.sql.template'), 'r').read()),
    11: Template(open(path.join(TEMPLATE_DIR, '11.sql.template'), 'r').read()),
    12: Template(open(path.join(TEMPLATE_DIR, '12.sql.template'), 'r').read()),
    13: Template(open(path.join(TEMPLATE_DIR, '13.sql.template'), 'r').read()),
    14: Template(open(path.join(TEMPLATE_DIR, '14.sql.template'), 'r').read()),
    15: Template(open(path.join(TEMPLATE_DIR, '15.sql.template'), 'r').read()),
    16: Template(open(path.join(TEMPLATE_DIR, '16.sql.template'), 'r').read()),
    17: Template(open(path.join(TEMPLATE_DIR, '17.sql.template'), 'r').read()),
    18: Template(open(path.join(TEMPLATE_DIR, '18.sql.template'), 'r').read()),
    19: Template(open(path.join(TEMPLATE_DIR, '19.sql.template'), 'r').read()),
    20: Template(open(path.join(TEMPLATE_DIR, '20.sql.template'), 'r').read()),
    21: Template(open(path.join(TEMPLATE_DIR, '21.sql.template'), 'r').read()),
    22: Template(open(path.join(TEMPLATE_DIR, '22.sql.template'), 'r').read()),
}


class Queries:
    def __init__(self, stream_id, connection):
        self.random = Random(stream_id)
        self.stream_id = stream_id
        with open('benchmarks/htap/queries/streams.yaml', 'r') as streams_file:
            sequence = yaml.load(streams_file.read(), Loader=yaml.FullLoader)[stream_id]
            self.next_query_it = cycle(sequence)
        self.conn = connection

    @staticmethod
    def query_ids():
        return sorted(QUERY_TEMPLATES.keys())

    @staticmethod
    def tpch_date_to_benchmark_date(tpch_date, highest_delivery_date):
        tpch_highest_delivery_date = isoparse('1998-12-01')
        delta = tpch_highest_delivery_date - tpch_date
        return highest_delivery_date - delta

    def _query_args(self, query_id, timestamp):
        if query_id == 1:
            tpch_date = isoparse('1998-12-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)
                            - timedelta(days=90)}
        elif query_id == 3:
            tpch_date = isoparse('1995-03-15')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 4:
            tpch_date = isoparse('1993-07-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 5:
            tpch_date = isoparse('1994-01-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 6:
            tpch_date = isoparse('1994-01-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 7 or query_id == 8:
            tpch_begin_date = isoparse('1995-01-01')
            tpch_end_date = isoparse('1996-12-31')
            return {'begin_date': self.tpch_date_to_benchmark_date(tpch_begin_date, timestamp),
                    'end_date': self.tpch_date_to_benchmark_date(tpch_end_date, timestamp)}
        elif query_id == 10:
            tpch_date = isoparse('1993-10-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 12:
            tpch_date = isoparse('1994-01-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 14:
            tpch_date = isoparse('1995-09-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 15:
            tpch_date = isoparse('1996-01-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        elif query_id == 20:
            tpch_date = isoparse('1994-01-01')
            return {'date': self.tpch_date_to_benchmark_date(tpch_date, timestamp)}
        else:
            return {}

    def get_query(self, query_id, timestamp):
        query_template = QUERY_TEMPLATES.get(query_id)
        query_args = self._query_args(query_id, timestamp)
        return query_template.substitute(**query_args)

    def run_next_query(self, benchmark_start_date, query_queue, dry_run):
        query_id = next(self.next_query_it)
        sql = self.get_query(query_id, benchmark_start_date)

        query_queue.put(('tpch', {
            'query': query_id,
            'stream': self.stream_id,
            'status': 'Running'
        }))

        if not dry_run:
            tstart = time.time()
            try:
                self.conn.cursor.execute(sql)
                status = 'Finished'
            except psycopg2.extensions.QueryCanceledError:
                status = 'Canceled'
            except psycopg2.ProgrammingError:
                status = 'Error'
            tstop = time.time()

            query_queue.put(('tpch', {
                'query': query_id,
                'stream': self.stream_id,
                'status': status,
                'runtime': tstop - tstart
            }))
        else:
            # Artificially slow down queries in dry-run mode to allow monitoring to keep up
            time.sleep(0.01)
            query_queue.put(('tpch', {
                'query': query_id,
                'stream': self.stream_id,
                'status': 'Finished',
                'runtime': 0.001
            }))
