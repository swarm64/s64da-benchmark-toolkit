import time
import os
import json

from datetime import timedelta, datetime
from dateutil.parser import isoparse
from itertools import cycle
from os import path
from string import Template

import psycopg2
import yaml

from .helpers import Random
from .helpers import TPCH_DATE_RANGE, WANTED_RANGE

from s64da_benchmark_toolkit.db import Status, DB, Timing

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

QUERY_IDS = sorted(QUERY_TEMPLATES.keys())

def is_ignored_query(ignored_queries, query_id):
    return (str(query_id) in ignored_queries)


class AnalyticalStream:
    def __init__(self, stream_id, args, min_timestamp, latest_timestamp, stats_queue):
        self.random = Random(stream_id)
        self.stream_id = stream_id
        with open('benchmarks/htap/queries/streams.yaml', 'r') as streams_file:
            sequence = yaml.load(streams_file.read(), Loader=yaml.FullLoader)[stream_id]
            self.next_query_it = cycle(sequence)
        self.min_timestamp = min_timestamp
        self.latest_timestamp = latest_timestamp
        self.stats_queue = stats_queue
        self.args = args
        if hasattr(args, "olap_dsns") and args.olap_dsns != None:
            self.dsn = args.olap_dsns[stream_id % len(args.olap_dsns)]
        else:
            self.dsn = args.dsn

    def tpch_date_to_benchmark_date(self, tpch_date):
        current_date = datetime.fromtimestamp(self.latest_timestamp.value)
        return current_date - (TPCH_DATE_RANGE[1] - tpch_date)

    def _query_args(self, query_id):
        if query_id == 1:
            return {'date': isoparse('1998-09-22')} # 1998-12-01 - 70 days
        elif query_id == 3:
            return {'date': isoparse('1995-03-07')}
        elif query_id == 4:
            return {'begin_date': isoparse('1994-01-01'), 
                    'end_date': isoparse('1994-04-01')}
        elif query_id == 5:
            return {'begin_date': isoparse('1993-01-01'), 
                    'end_date': isoparse('1994-01-01')}
        elif query_id == 6:
            return {'begin_date': isoparse('1993-01-01'), 
                    'end_date': isoparse('1994-01-01')}
        elif query_id == 7 or query_id == 8:
            return {'begin_date': isoparse('1995-01-01'), 
                    'end_date': isoparse('1996-12-31')}
        elif query_id == 10:
            return {'begin_date': isoparse('1993-07-01'), 
                    'end_date': isoparse('1993-10-01')}
        elif query_id == 12:
            return {'begin_date': isoparse('1996-01-01'), 
                    'end_date': isoparse('1997-01-01')}
        elif query_id == 14:
            return {'begin_date': isoparse('1996-01-01'), 
                    'end_date': isoparse('1996-02-01')}
        elif query_id == 15:
            return {'begin_date': isoparse('1995-10-01'), 
                    'end_date': isoparse('1996-01-01')}
        elif query_id == 20:
            return {'begin_date': isoparse('1994-01-01'), 
                    'end_date': isoparse('1995-04-01')}
        else:
            return {}

    def get_query(self, query_id):
        query_template = QUERY_TEMPLATES.get(query_id)
        query_args = self._query_args(query_id)
        for [arg, date] in query_args.items():
            query_args[arg] = self.tpch_date_to_benchmark_date(date)
        query_args['min_date'] = self.tpch_date_to_benchmark_date(TPCH_DATE_RANGE[0])
        return query_template.substitute(**query_args)

    def wait_until_enough_data(self, query_id):
        while True:
            available_data = datetime.fromtimestamp(self.latest_timestamp.value) - self.min_timestamp
            if available_data < WANTED_RANGE:
                self.stats_queue.put(('olap', {
                    'query': query_id,
                    'stream': self.stream_id,
                    'status': 'IGNORED' if is_ignored_query(self.args.ignored_queries, query_id) else 'Waiting'
                }))
                time.sleep(1)
            else:
                return

    def parse_plan(self, plan):
        planned = plan["Plan Rows"] if "Plan Rows" in plan else 0
        processed = plan["Actual Rows"] if "Actual Rows" in plan else 0
        if "Plans" in plan:
            for child in plan["Plans"]:
                planned_child, processed_child = self.parse_plan(child)
                planned += planned_child
                processed += processed_child
        return (planned, processed)


    def run_next_query(self):
        query_id = next(self.next_query_it)
        if is_ignored_query(self.args.ignored_queries, query_id):
            self.stats_queue.put(('olap', {
                'query': query_id,
                'stream': self.stream_id,
                'status': 'IGNORED'
            }))
            return
        sql = self.get_query(query_id)

        if not self.args.dont_wait_until_enough_data:
            self.wait_until_enough_data(query_id)

        self.stats_queue.put(('olap', {
            'query': query_id,
            'stream': self.stream_id,
            'status': 'Running'
        }))

        if not self.args.dry_run:
            db = DB(self.dsn)
            timing, _, plan = DB(self.dsn).run_query(
                    sql, self.args.olap_timeout,
                    self.args.explain_analyze, self.args.use_server_side_cursors)

            # sum up rows processed
            try:
                planned_rows, processed_rows = self.parse_plan(json.loads(plan)[0]["Plan"])
            except:
                planned_rows = 0
                processed_rows = 0

            self.stats_queue.put(('olap', {
                'query': query_id,
                'stream': self.stream_id,
                'status': timing.status.name,
                'runtime': timing.stop - timing.start,
                'planned_rows': planned_rows,
                'processed_rows': processed_rows
            }))

            # save plan output
            plan_file =  f'{self.stream_id}_{query_id}.txt'
            plan_dir = f'results/query_plans'
            os.makedirs(plan_dir, exist_ok=True)
            with open(f'{plan_dir}/{plan_file}', 'w') as f:
                f.write(plan)
        else:
            # Artificially slow down queries in dry-run mode to allow monitoring to keep up
            time.sleep(0.01)
            self.stats_queue.put(('olap', {
                'query': query_id,
                'stream': self.stream_id,
                'status': 'OK',
                'runtime': 0.001
            }))
