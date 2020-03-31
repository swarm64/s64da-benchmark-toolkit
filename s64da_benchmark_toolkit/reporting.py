
import logging
import os

from csv import writer as csv_writer
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas

from natsort import index_natsorted, order_by_index
from tabulate import tabulate

from .correctness import Correctness, CorrectnessResult
from .netdata import Netdata


LOG = logging.getLogger()


class QueryMetric:
    dataframe_columns = (
        'stream_id', 'query_id', 'timestamp_start', 'timestamp_stop',
        'runtime', 'status')

    def __init__(self, *, stream_id, query_id, timestamp_start, timestamp_stop,
                 status, result, plan):
        self.stream_id = stream_id
        self.query_id = query_id
        self.timestamp_start = datetime.fromtimestamp(timestamp_start)
        self.timestamp_stop = datetime.fromtimestamp(timestamp_stop)
        self.status = status
        self.result = result
        self.plan = plan

    def make_file_name(self, extension):
        return f'{self.stream_id}_{self.query_id}.{extension}'

    @property
    def dataframe(self):
        runtime = (self.timestamp_stop - self.timestamp_start).total_seconds()
        return pandas.DataFrame(data=[[
            self.stream_id, self.query_id, self.timestamp_start,
            self.timestamp_stop, runtime, self.status
        ],], columns=QueryMetric.dataframe_columns)


class Reporting:
    def __init__(self, benchmark, args, config):
        self.uuid = uuid4()
        self.benchmark = benchmark
        self.results_root_dir = 'results'
        self.html_output = os.path.join(self.results_root_dir, 'report.html')
        self.query_results = os.path.join(self.results_root_dir, 'query_results')
        self.explain_analyze_dir = os.path.join(self.results_root_dir, 'query_plans')
        self.scale_factor = args.scale_factor
        self.explain_analyze = args.explain_analyze
        self.all_query_metrics = []
        self.netdata_output_file = args.netdata_output_file
        self.config = config

        self.output = args.output
        self.csv_file = args.csv_file
        if 'csv' in self.output:
            Path(self.csv_file).touch()

        if args.check_correctness and not os.path.exists(self.results_root_dir):
            try:
                os.makedirs(self.results_root_dir)
            except OSError as exc:
                LOG.exception(f'Could not create directory {self.results_root_dir}')

    def run_report(self, reporting_queue):
        df = pandas.DataFrame(columns=QueryMetric.dataframe_columns)

        while not reporting_queue.empty():
            query_metric = reporting_queue.get()

            if self.explain_analyze:
                self._save_explain_plan(query_metric)

            if self.scale_factor:
                self._save_query_output(query_metric)

            df = df.append(query_metric.dataframe)

        df = df.reset_index(drop=True)

        netdata_config = self.config.get('netdata')
        if netdata_config:
            netdata = Netdata(netdata_config)
            if self.netdata_output_file:
                netdata.write_stats(df, self.netdata_output_file)

        if self.scale_factor:
            df = self._check_correctness(df)

        total_runtime = df['timestamp_stop'].max() - df['timestamp_start'].min()
        total_runtime_seconds = total_runtime.total_seconds()
        self._print_results(df, total_runtime_seconds)

        print(f'\nTotal runtime: {total_runtime} ({total_runtime_seconds:.2f}s)')

    def _save_explain_plan(self, query_metric):
        plan_file_name = query_metric.make_file_name('txt')
        plan_file_path = os.path.join(self.explain_analyze_dir, plan_file_name)
        os.makedirs(os.path.dirname(plan_file_path), exist_ok=True)

        with open(plan_file_path, 'w') as plan_file:
            plan_file.write(query_metric.plan)

    def _save_query_output(self, query_metric):
        query_result = query_metric.result
        if query_result is not None and query_result[1]:
            query_result_header = query_result[0]
            query_result_data = query_result[1]

        else:
            query_result_header = []
            query_result_data = []

        csv_file_name = query_metric.make_file_name('csv')
        csv_file_path = os.path.join(self.query_results, csv_file_name)
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        with open(csv_file_path, 'w') as csv_file:
            csvfile = csv_writer(csv_file)
            csvfile.writerow(query_result_header)
            csvfile.writerows(query_result_data)

    @classmethod
    def _sort_df(cls, df):
        index_sort = index_natsorted(zip(df['stream_id'], df['query_id']))
        df = df.reindex(index=order_by_index(df.index, index_sort))
        df = df.reset_index(drop=True)
        return df

    def _print_results(self, df, total_runtime_seconds):
        df = Reporting._sort_df(df)

        if 'print' in self.output:
            print('\n')
            print(tabulate(df, headers='keys', tablefmt='github', floatfmt='.2f'))

        if 'csv' in self.output:
            if self.csv_file:
                df.to_csv(self.csv_file, sep=';')

        if 'correctness_check' in df:
            report_datetime = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            with open(self.html_output, 'w') as html_output_file:
                html_output_file.write(f'''
<h1>Swarm64 Benchmark Results Report</h1>
<p>{ report_datetime }</p>
<p>Total runtime: { total_runtime_seconds }s</p>\n''')
                df.to_html(buf=html_output_file, escape=False, formatters={
                    'correctness_check': CorrectnessResult.format_html
                })

    def _check_correctness(self, df):
        correctness = Correctness(self.scale_factor, self.benchmark.name)
        stream_ids = tuple(df['stream_id'].drop_duplicates())

        df['correctness_check'] = None
        for stream_id in stream_ids:
            sub_df = df.loc[df['stream_id'] == stream_id]
            for index, row in sub_df.iterrows():
                if row['status'] == 'OK':
                    query_id = row['query_id']
                    df.loc[index, 'correctness_check'] = correctness.check_correctness(stream_id, query_id)

        return df
