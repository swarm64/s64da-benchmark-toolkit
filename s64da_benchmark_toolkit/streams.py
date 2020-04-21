# -*- coding: utf-8 -*-

import logging
import os
import csv
import time

from collections import namedtuple
from datetime import datetime
from multiprocessing import Manager, Pool
from natsort import natsorted
from pandas.io.formats.style import Styler

import pandas
import yaml

from .db import DB
from .reporting import Reporting, QueryMetric


Benchmark = namedtuple('Benchmark', ['name', 'base_dir'])


LOG = logging.getLogger()


class Streams:
    def __init__(self, args, benchmark):

        # The Output structure:
        #
        # s64da-benchmark-toolkit/
        # ├── results/
        # │   ├── results.csv
        # │   ├── report.html
        # │   ├── query_results/
        # │   │   ├── 0_1.csv
        # │   │   ├── 0_2.csv
        # │   ├── query_plans/
        # │   │   ├── 0_1.txt
        # │   │   ├── 0_2.txt

        self.config = Streams._make_config(args, benchmark)
        self.db = DB(args.dsn)
        self.num_streams = args.streams
        self.benchmark = benchmark
        self.stream_offset = args.stream_offset
        self.scale_factor = args.scale_factor
        self.query_dir = self._get_query_dir()
        self.explain_analyze = args.explain_analyze
        self.reporting = Reporting(benchmark, args, self.config)

    @staticmethod
    def _make_config(args, benchmark):
        config = {}
        config_file = args.config or f'benchmarks/{benchmark.name}/configs/default.yaml'

        with open(config_file, 'r') as conf_file:
            config = yaml.load(conf_file, Loader=yaml.Loader)

        if args.timeout:
            config['timeout'] = args.timeout

        return config

    def _get_query_dir(self):
        _dir = os.path.join(self.benchmark.base_dir, 'queries')
        if os.path.isdir(os.path.join(_dir, f'queries_{self.scale_factor}')):
            _dir = os.path.join(_dir, f'queries_{self.scale_factor}')
        return _dir

    def read_sql_file(self, query_id):
        query_path = os.path.join(self.query_dir, f'{query_id}.sql')
        with open(query_path, 'r') as query_file:
            return query_file.read()

    @staticmethod
    def apply_sql_modifications(sql, modifications):
        for modification in modifications:
            sql = sql.replace(modification[0], modification[1])
        return sql

    def run(self):
        try:
            mp_manager = Manager()
            reporting_queue = mp_manager.Queue()

            self.db.reset_config()
            self.db.apply_config(self.config.get('dbconfig', {}))

            self.run_streams(reporting_queue)
            self.reporting.run_report(reporting_queue)

        except KeyboardInterrupt:
            # Reset all the stuff
            pass

        finally:
            self.db.reset_config()

    def get_stream_sequence(self, stream_id):
        streams_path = os.path.join(self.benchmark.base_dir, 'queries', 'streams.yaml')
        with open(streams_path, 'r') as streams_file:
            try:
                return yaml.load(streams_file, Loader=yaml.Loader)[stream_id]
            except KeyError:
                raise ValueError(f'Stream file {streams_path} does not contain stream id {stream_id}')

    def _make_run_args(self, reporting_queue):
        if self.num_streams == 0:
            return ((reporting_queue, 0),)
        else:
            return tuple((reporting_queue, stream) for stream in
                         range(self.stream_offset, self.num_streams + self.stream_offset))

    def run_streams(self, reporting_queue):
        with Pool(processes=max(self.num_streams, 1)) as pool:
            map_args = self._make_run_args(reporting_queue)
            pool.starmap(self._run_stream, map_args)

    def _run_query(self, stream_id, query_id):
        query_sql = self.read_sql_file(query_id)
        query_sql = Streams.apply_sql_modifications(query_sql, (
            ('revenue0', f'revenue{stream_id}'),))
        timeout = self.config.get('timeout', 0)
        return self.db.run_query(query_sql, timeout, self.explain_analyze)

    def _run_stream(self, reporting_queue, stream_id):
        sequence = self.get_stream_sequence(stream_id)
        num_queries = len(sequence)
        for idx, query_id in enumerate(sequence):
            num_query = idx + 1
            pretext = f'{num_query:2}/{num_queries:2}: query {query_id:2} of stream {stream_id:2}'

            if query_id in self.config.get('ignore', []):
                LOG.info(f'ignoring {pretext}.')
                continue

            LOG.info(f'running  {pretext}.')
            timing, query_result, plan = self._run_query(stream_id, query_id)

            runtime = timing.stop - timing.start
            LOG.info(f'finished {pretext}: {runtime:.2f}s {timing.status.name}')

            reporting_queue.put(QueryMetric(
                stream_id=stream_id,
                query_id=query_id,
                timestamp_start=timing.start,
                timestamp_stop=timing.stop,
                status=timing.status.name,
                result=query_result,
                plan=plan
            ))
