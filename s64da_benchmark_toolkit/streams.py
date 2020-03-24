# -*- coding: utf-8 -*-

import logging
import os
import csv
import time

from collections import namedtuple
from datetime import datetime
from multiprocessing import Pool
from natsort import natsorted
from pandas.io.formats.style import Styler
from pathlib import Path

import pandas
import yaml

from .db import DB
from .correctness import Correctness
from .netdata import Netdata


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

        self.output = args.output
        self.csv_file = args.csv_file
        if 'csv' in self.output:
            Path(os.path.dirname(self.csv_file)).mkdir(parents=True, exist_ok=True)
            Path(self.csv_file).touch()

        self.results_root_dir = 'results'
        self.html_output = os.path.join(self.results_root_dir, 'report.html')
        self.query_results = os.path.join(self.results_root_dir, 'query_results')
        self.explain_analyze_dir = os.path.join(self.results_root_dir, 'query_plans')

        if args.check_correctness and not os.path.exists(self.results_root_dir):
            try:
                os.makedirs(self.results_root_dir)
            except OSError as exc:
                LOG.exception(f'Could not create directory {self.results_root_dir}')

        self.config = Streams._make_config(args, benchmark)
        self.db = DB(args.dsn)
        self.num_streams = args.streams
        self.netdata_output_file = args.netdata_output_file
        self.benchmark = benchmark
        self.stream_offset = args.stream_offset
        self.scale_factor = args.scale_factor
        self.query_dir = self._get_query_dir()

        self.explain_analyze = args.explain_analyze

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

    @staticmethod
    def sort_df(df):
        return df.reindex(index=natsorted(df.index))

    def save_to_dataframe(self, results):
        df = pandas.DataFrame()

        for column in results:
            key = list(column.keys())[0]
            columns = [f'{key} start', f'{key} stop', f'{key} status']

            _df = pandas.DataFrame(data=column[key]).transpose()
            _df = Streams.sort_df(_df)
            _df.columns = columns

            df[f'Stream {key:02} metric'] = (_df[columns[1]] - _df[columns[0]]).apply(lambda x: round(x, 2))
            df[f'Stream {key:02} status'] = _df[columns[2]].transform(lambda x: x.name)

        df.index = _df.index
        df.index.name = 'Query'
        return df

    def add_correctness(self, results_dataframe):
        if self.scale_factor:
            stream_ids = range(self.stream_offset, self.num_streams + self.stream_offset)
            if self.num_streams == 0:
                stream_ids = [0]
            cc = Correctness(self.scale_factor, self.benchmark.name)
            for query_number in results_dataframe.index:
                for stream_id in stream_ids:
                    if results_dataframe.at[query_number, f'Stream {stream_id:02} status'] == 'OK':
                        results_dataframe.at[query_number, f'Stream {stream_id:02} status'] =\
                            cc.check_correctness(stream_id, query_number)

            # Save results to html if correctness check is on
            self.save_results_to_html(results_dataframe, cc.html)

        return results_dataframe

    def _print_results(self, results):
        if 'print' in self.output:
            with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
                print(results)
        if 'csv' in self.output:
            if self.csv_file:
                results.to_csv(self.csv_file, sep=';')
        if not self.output:
            raise ValueError(f'No output format was defined.')

    def run(self):
        try:
            self.db.reset_config()
            self.db.apply_config(self.config.get('dbconfig', {}))

            totalstart = time.perf_counter()
            results = self.run_streams()
            totalstop = time.perf_counter()
            results_df = self.save_to_dataframe(results)

            netdata_config = self.config.get('netdata')
            if netdata_config and self.netdata_output_file:
                if self.num_streams <= 1:
                    netdata = Netdata(netdata_config)
                    netdata.write_stats(results[0][0], self.netdata_output_file)
                else:
                    LOG.info('Running more than one stream. Not retrieving netdata stats.')

            results_with_correctness = self.add_correctness(results_df)
            self._print_results(results_with_correctness)
            LOG.info(f'Total time spent: {totalstop - totalstart:.2f} secs.')

        except KeyboardInterrupt:
            # Reset all the stuff
            pass

        finally:
            self.db.reset_config()

    def get_stream_sequence(self, stream_id):
        streams_path = os.path.join(self.benchmark.base_dir, 'queries', 'streams.yaml')
        with open(streams_path, 'r') as streams_file:
            return yaml.load(streams_file, Loader=yaml.Loader)[stream_id]

    def _make_run_args(self):
        if self.num_streams == 0:
            return ((0,),)
        else:
            return tuple((stream,) for stream in range(self.stream_offset, self.num_streams + self.stream_offset))

    def run_streams(self):
        with Pool(processes=max(self.num_streams, 1)) as pool:
            map_args = self._make_run_args()
            return pool.starmap(self._run_stream, map_args)

    def _run_stream(self, stream_id):
        sequence = self.get_stream_sequence(stream_id)

        timings = {}
        num_queries = len(sequence)
        for idx, query_id in enumerate(sequence):
            num_query = idx + 1
            pretext = f'{num_query:2}/{num_queries:2}: query {query_id:2} of stream {stream_id:2}'

            if query_id in self.config.get('ignore', []):
                LOG.info(f'ignoring {pretext}.')
                continue

            query_sql = self.read_sql_file(query_id)
            query_sql = Streams.apply_sql_modifications(query_sql, (('revenue0', f'revenue{stream_id}'),))

            LOG.info(f'running  {pretext}.')
            timing, query_result = self.db.run_query(query_sql, self.config.get('timeout', 0), self.explain_analyze)

            if self.explain_analyze:
                self._save_explain_plan(stream_id, query_id, self.db.plan)

            if self.scale_factor:
                self._save_query_output(stream_id, query_id, query_result)

            runtime = round(timing.stop - timing.start, 2)
            LOG.info(f'finished {pretext}: {runtime:7.2f} - {timing.status.name}')

            timings[query_id] = timing

        return {stream_id: timings}

    def _save_query_output(self, stream_id, query_id, query_result):

        filename = os.path.join(self.query_results, f'{stream_id}_{query_id}.csv')
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        if query_result is not None and query_result[1]:
            query_result_header = query_result[0]
            query_result_data = query_result[1]

        else:
            query_result_header = []
            query_result_data = []

        with open(filename, 'w') as f:
            csvfile = csv.writer(f)
            csvfile.writerow(query_result_header)
            csvfile.writerows(query_result_data)

    def _save_explain_plan(self, stream_id, query_id, plan):
        filename = os.path.join(self.explain_analyze_dir, f'{stream_id}_{query_id}.txt')
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, 'w') as f:
            f.write(plan)

    def save_results_to_html(self, results, correctness_html=None):

        html = f'<h1> Swarm64 Benchmark Results Report </h1><p>{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}</p>'

        Swarm64Styler = Styler.from_custom_template("resources", "report.tpl")
        html += Swarm64Styler(results).render()

        if correctness_html:
            html += correctness_html

        with open(self.html_output, 'w') as f:
            f.write(html)
