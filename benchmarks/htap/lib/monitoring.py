import time
import math
import os
import psycopg2
from copy import deepcopy

from datetime import datetime
from dateutil.relativedelta import relativedelta
from math import trunc
from uuid import uuid4
from collections import defaultdict
from urllib.parse import urlparse

from psycopg2.extras import register_uuid

from .queries import Queries
from s64da_benchmark_toolkit.dbconn import DBConn

register_uuid()

QUERY_TYPES = ['ok', 'error', 'new_order', 'payment', 'order_status', 'delivery', 'stock_level']
HISTORY_LENGTH = 10

class Stats:
    """
    A stats collector for TPC-C and TPC-H stats.
    The way TPC-C and TPC-H report their stats are quite different.

    TPC-C
    -----
    Since TPC-C has a very high througput (thousands per second) only
    the end state of a transaction is sent through the queue. Per reporting interval
    all accumulated events are sent, which contain the timestamp, query id and runtime
    for each transaction.
    This is kept in a list where the last HISTORY_LENGTH seconds of samples are stored.
    When we want to report the actual statistics we then simply aggregate the results
    based on the raw, possibly filtered, samples.

    TPC-H
    -----
    For TPC-H the status of each individual query (Running, Ok, Error,
    Timeout) is put into the monitoring queue, regardless of monitoring interval.
    """
    def __init__(self, dsn, num_oltp_slots, num_olap_slots, csv_interval):
        self.data = {}
        self.data['tpcc'] = defaultdict(list)
        self.data['tpcc_totals'] = defaultdict(int)
        self.data['tpch'] = [{
                'queries': {query: {'status': 'Waiting', 'runtime': 0} for query in Queries.query_ids()},
                'ok_count': 0,
                'timeout_count': 0,
                'error_count': 0,
                'ignored_count': 0
        } for _ in range(num_olap_slots)]
        self.uuid = uuid4()
        self.num_oltp_slots = num_oltp_slots
        self.updates = 0
        self.csv_interval = csv_interval
        self.dsn = dsn
        self.database = urlparse(self.dsn).path[1:]
        self.csv_tpch = None
        self.csv_tpcc = None
        self.csv_dbstats = None
        self.conn = None
        
    def _update_tpcc_stats(self, stats):
        for stat in stats:
            self.data['tpcc'][stat['query']].append(stat)
            self.data['tpcc_totals'][stat['query']] += 1

    def cleanup_tpcc_stats(self, time_now):
        cutoff = int(time_now - HISTORY_LENGTH)
        for query_type in self.data['tpcc'].keys():
            # keep one minute of history
            self.data['tpcc'][query_type] = list(filter(lambda x: x['timestamp'] >= cutoff, self.data['tpcc'][query_type]))

    def _update_tpch_stats(self, stats):
        stream_id = stats['stream']
        query_id = stats['query']

        if stats['status'] == 'Waiting' or stats['status'] == 'Running' or stats['status'] == 'IGNORED':
            # only overwrite status so we keep the last runtime
            self.data['tpch'][stream_id]['queries'][query_id]['last-status'] = \
                self.data['tpch'][stream_id]['queries'][query_id]['status']
            self.data['tpch'][stream_id]['queries'][query_id]['status'] = stats['status']
            return

        self.data['tpch'][stream_id]['queries'][query_id] = stats
        if self.csv_tpch:
            self.csv_tpch.write(f'{stream_id}, {query_id}, {stats["status"]}, {stats["runtime"]}\n')
            self.csv_tpch.flush()
        status_count = stats['status'].lower() + '_count'
        self.data['tpch'][stream_id][status_count] += 1

    def _process_queue(self, src, item):
        if src == 'tpcc':
            self._update_tpcc_stats(item)
        elif src == 'tpch':
            self._update_tpch_stats(item)

    def _update_cached_stats(self):
        with self.conn as conn:
            try:
                conn.cursor.execute("select table_name,relation_blocks,compressed_blocks,cache_pages_usable from swarm64da.stat_all_column_store_indexes")
                self.cached_columnstore_stats = conn.cursor.fetchall()
            except:
                self.cached_columnstore_stats = []
            self.conn.cursor.execute(f"select pg_database_size('{self.database}')")
            self.cached_database_size = conn.cursor.fetchone()[0]

    def process_queue(self, reporting_queue):
        while not reporting_queue.empty():
            src, item = reporting_queue.get()
            self._process_queue(src, item)

    def update(self):
        # open the files and connections lazily to avoid serialization problems when forking
        os.makedirs('results', exist_ok=True)
        if not self.csv_tpcc:
            self.csv_tpcc = open('results/tpcc.csv', 'w')
        if not self.csv_tpch:
            self.csv_tpch = open('results/tpch.csv', 'w')
        if not self.csv_dbstats:
            self.csv_dbstats = open('results/dbstats.csv', 'w')
        if not self.conn:
            self.conn = DBConn(self.dsn, use_dict_cursor = True)
            self._update_cached_stats()

        self.cleanup_tpcc_stats(time.time())

        self.updates += 1
        if self.updates % 10 == 0:
            self._update_cached_stats()
        if self.csv_interval and self.updates % self.csv_interval == 0:
            self.write_tpcc_stats()
            self.write_dbstats()

    def tpch_stats_for_stream_id(self, stream_id):
        return self.data['tpch'][stream_id]

    def filter_last_1s(self, stats):
        ts = max(stat['timestamp'] for stat in stats) - 1
        return list(filter(lambda x: x['timestamp'] >= ts, stats))

    def tpcc_tps(self, query):
        if not query in self.data['tpcc']:
            return (0, 0, 0, 0)

        stats = self.data['tpcc'][query]
        per_sec = defaultdict(int)

        time_now = int(time.time())
        # fill up the whole timestamp range, but not the first and last second
        for timestamp in range(time_now-HISTORY_LENGTH+1, time_now-1):
            per_sec[timestamp] = 0
        for stat in stats:
            bucket = int(stat['timestamp'])
            per_sec[bucket] += 1
        tps = list(per_sec.values())
        if len(tps) <= 2:
            return (0, 0, 0, 0)

        # first and last second will not be complete so ignore those
        tps.pop(0)
        tps.pop(-1)
        return (tps[-1], min(tps), int(sum(tps)/len(tps)), max(tps))

    def tpcc_latency(self, query):
        if not query in self.data['tpcc']:
            return (0, 0, 0, 0)

        stats = self.data['tpcc'][query]
        if len(stats) == 0:
            return (0, 0, 0, 0)

        lat = list(map(lambda x: math.ceil(x['runtime']*1000), stats))
        lat_1s = list(map(lambda x: math.ceil(x['runtime']*1000), self.filter_last_1s(stats)))
        return (int(sum(lat_1s)/len(lat_1s)),
                min(lat),
                int(sum(lat)/len(lat)),
                max(lat))

    def tpcc_total(self, query_type):
        return self.data['tpcc_totals'][query_type]

    def tpch_totals(self):
        return tuple(sum(slot[slot_type] for slot in self.data['tpch'])
                for slot_type in ['ok_count', 'error_count', 'timeout_count'])

    def db_size(self):
        return "{:7.2f}GB".format(self.cached_database_size / (1024*1024*1024.0))

    def columnstore_stats(self):
        result = []
        for row in self.cached_columnstore_stats:
            table_name = row['table_name']
            table_size = max(1, row['relation_blocks']*8/1024/1024);
            index_size = max(1, row['compressed_blocks']*8/1024/1024);
            compression_ratio = table_size * 1.0 / index_size
            cached = row['cache_pages_usable'] * 1.0 / max(1, row['relation_blocks']) * 100
            result.append([table_name, table_size, index_size, compression_ratio, cached])
        return result

    def write_tpcc_stats(self):
        for query_type in QUERY_TYPES:
            row = [datetime.now(), query_type, self.tpcc_total(query_type), *self.tpcc_tps(query_type), *self.tpcc_latency(query_type)]
            self.csv_tpcc.write(', '.join(map(str, row)) + "\n")
        self.csv_tpcc.flush()

    def write_dbstats(self):
        for row in self.columnstore_stats():
            self.csv_dbstats.write(', '.join(map(str, row)) + "\n")
        self.csv_dbstats.flush()

    def write_summary(self, csv_file, elapsed):
        row_nr = 0
        with open(csv_file, 'w') as csv:
            csv.write(';stream_id;query_id;timestamp_start;timestamp_stop;runtime;status;correctness_check\n')
            # oltp stream = stream 0
            stream_id = 0
            fake_date = datetime.now()
            elapsed_seconds = elapsed.total_seconds()
            for query_type in QUERY_TYPES:
                total = self.tpcc_total(query_type)
                tps = self.tpcc_tps(query_type)[2]
                latency = self.tpcc_latency(query_type)[2]
                csv.write(f'{row_nr};{stream_id};{query_type}_total;{fake_date};{fake_date};{total};OK;OK\n')
                row_nr += 1
                csv.write(f'{row_nr};{stream_id};{query_type}_tps;{fake_date};{fake_date};{tps};OK;OK\n')
                row_nr += 1
                csv.write(f'{row_nr};{stream_id};{query_type}_latency;{fake_date};{fake_date};{latency};OK;OK\n')
                row_nr += 1
 
            # now all olap streams
            for stream_idx, stream in enumerate(self.data['tpch']):
                for query_id, stats in stream['queries'].items():
                    stream_id = stream_idx + 1 # so that the oltp stream can be 0
                    status = stats["status"].upper()
                    runtime = stats["runtime"]
                    if status == "RUNNING" and "last-status" in stats:
                        # query is currently running, take the last status
                        status = stats["last-status"].upper()
                    if not status in ["OK", "ERROR", "TIMEOUT"]:
                        # anything that has not run (yet) we just set to timeout as
                        # there was apparently no time to run it
                        status = "TIMEOUT"
                    csv.write(f'{row_nr};{stream_id};{query_id};{fake_date};{fake_date};{runtime};{status};{status}\n')
                    row_nr += 1

class Monitor:
    def __init__(self, stats, num_oltp_workers, num_olap_workers, scale_factor, min_timestamp):
        self.stats = stats
        self.num_oltp_workers = num_oltp_workers
        self.num_olap_workers = num_olap_workers
        self.scale_factor = scale_factor
        self.output = None
        self.current_line = 0
        self.total_lines = 0
        self.min_timestamp = min_timestamp.date()
        self.lines = []

    def _add_display_line(self, line):
        self.lines.append(line)

    def _print(self):
        print(self.total_lines * '\033[F', end='')
        for line in self.lines:
            print('\033[2K', end='')
            print(line)
        self.total_lines = len(self.lines)
        self.lines = []

    def display_summary(self, elapsed):
        elapsed_seconds = max(1, elapsed.total_seconds())
        tps = self.stats.tpcc_total('ok')    / elapsed_seconds
        eps = self.stats.tpcc_total('error') / elapsed_seconds
        tpmc = trunc((self.stats.tpcc_total('new_order') / elapsed_seconds) * 60)
        num_queries, num_errors, num_timeouts = self.stats.tpch_totals()
        throughput = num_queries * 3600 / elapsed_seconds
        print()
        summary = 'Summary'
        print(f'{summary}\n' + len(summary) * '-')
        print(f'Scale Factor: {self.scale_factor}')
        print(f'Streams: {self.num_oltp_workers} OLTP, {self.num_olap_workers} OLAP')
        print(f'Total time: {elapsed_seconds:.2f} seconds')
        print(f'TPC-C AVG Transactions per second (TPS): {tps:.2f}')
        print(f'TPC-C AVG Errors per second: {eps:.2f}')
        print(f'TPC-C New-Order transactions per minute (tpmC): {tpmc:.0f}')
        print(f'TPC-H Throughput (queries per hour): {throughput:.1f}')
        print(f'TPC-H Errors {num_errors:}, Timeouts: {num_timeouts:}')

    def get_elapsed_row(self, elapsed_seconds):
        unit = 'second' if elapsed_seconds < 2 else 'seconds'
        return f'Elapsed: {elapsed_seconds:.0f} {unit}'

    def get_tpcc_row(self, query_type):
        total = self.stats.tpcc_total(query_type)
        tps     = '{:5} | {:5} | {:5} | {:5}'.format(*self.stats.tpcc_tps(query_type))
        latency = '{:5} | {:5} | {:5} | {:5}'.format(*self.stats.tpcc_latency(query_type))
        return f'| {query_type:^12} | {total:8} | {tps} | {latency} |'

    def get_olap_header(self):
        olap_header = f'{"Stream":<8} |'
        olap_header += ''.join([f'{x:^10d} |' for x in range(1, self.num_olap_workers + 1)])
        olap_header += f' {"#rows planned":13} | {"#rows processed":14} |'
        return olap_header

    def get_olap_row(self, query_id):
        row = f'Query {query_id:2d} |'
        max_planned = 0
        max_processed = 0
        for stream_id in range(self.num_olap_workers):
            stats = self.stats.tpch_stats_for_stream_id(stream_id).get('queries').get(query_id)
            if stats and stats['runtime'] > 0:
                # output last result
                max_planned = max(max_planned, int(stats['planned_rows']/1000))
                max_processed = max(max_processed, int(stats['processed_rows']/1000))
                row += '{:7.2f} {:3}|'.format(stats['runtime'], stats['status'][:3].upper())
            elif stats:
                # output a state
                row += ' {:^10}|'.format(stats['status'])
            else:
                row += f' {" ":9} |'

        row += f' {max_planned:12}K | {max_processed:13}K |'
        return row

    def get_olap_sum(self):
        row = f'SUM      |'
        for stream_id in range(self.num_olap_workers):
            stats = self.stats.tpch_stats_for_stream_id(stream_id)
            stream_sum = sum(stats['queries'][query_id]['runtime'] for query_id in Queries.query_ids())
            row += f' {stream_sum:9.2f} |'
        return row

    def get_columnstore_row(self, row):
        return f'| {row[0]:^12} | {row[1]:7.2f}GB | {row[2]:7.2f}GB | {row[3]:6.2f}x | {row[4]:4.2f}% |'

    def update_display(self, time_elapsed, time_now, stats_conn, latest_timestamp):
        latest_time = latest_timestamp.date()
        date_range = relativedelta(latest_time, self.min_timestamp)

        self.current_line = 0
        data_warning = "(not enough for consistent OLAP queries)" if date_range.years < 7 else ""

        self._add_display_line(f'Data range: {self.min_timestamp} - {latest_time} = {date_range.years} years, {date_range.months} months and {date_range.days} days {data_warning}')
        self._add_display_line(f'DB size: {self.stats.db_size()}')
        if self.stats.columnstore_stats():
            self._add_display_line('-----------------------------------------------------------')
            self._add_display_line('|    table     | heap size |  colstore |  ratio  | cached |')
            self._add_display_line('-----------------------------------------------------------')
            for row in self.stats.columnstore_stats():
                self._add_display_line(self.get_columnstore_row(row))

        self._add_display_line('-------------------------------------------------------------------------------------------')
        self._add_display_line('|     TYPE     |  TOTALS  |         TPS (last {}s)        |   LATENCY (last {}s, in ms)   |'.format(HISTORY_LENGTH, HISTORY_LENGTH))
        self._add_display_line('|              |          |  CURR |  MIN  |  AVG  |  MAX  |  CURR |  MIN  |  AVG  |  MAX  |')
        self._add_display_line('|------------------------------------------------------------------------------------------')
        for query_type in QUERY_TYPES:
            self._add_display_line(self.get_tpcc_row(query_type))
        self._add_display_line('|------------------------------------------------------------------------------------------')

        if self.num_olap_workers > 0:
            self._add_display_line('')
            olap_header = self.get_olap_header()
            self._add_display_line(olap_header)
            self._add_display_line('-' * len(olap_header))

            for query_id in Queries.query_ids():
                self._add_display_line(self.get_olap_row(query_id))
            self._add_display_line(self.get_olap_sum())

        self._add_display_line('')
        self._add_display_line(self.get_elapsed_row(time_elapsed))
        self._print()
