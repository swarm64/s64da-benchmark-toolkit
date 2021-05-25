import time
import math
import os

from datetime import datetime
from uuid import uuid4
from collections import defaultdict
from urllib.parse import urlparse

from psycopg2.extras import register_uuid

from benchmarks.htap.lib.analytical import QUERY_IDS, is_ignored_query
from s64da_benchmark_toolkit.dbconn import DBConn

register_uuid()

QUERY_TYPES = ['new_order', 'payment', 'order_status', 'delivery', 'stock_level']
HISTORY_LENGTH = 10

class Stats:
    """
    A stats collector for OLTP and OLAP stats.
    The way OLTP and OLAP workloads report their stats are quite different.

    OLTP
    -----
    Since OLTP has a very high througput (thousands per second) only
    the end state of a transaction is sent through the queue. Per reporting interval
    all accumulated events are sent, which contain the timestamp, query id, status, and runtime
    for each transaction.
    This is kept in a list where the last HISTORY_LENGTH seconds of samples are stored.
    When we want to report the actual statistics we then simply aggregate the results
    based on the raw, possibly filtered, samples.

    OLAP
    -----
    For OLAP the status of each individual query (Running, Ok, Error, Ignored
    Timeout) is put into the monitoring queue, regardless of monitoring interval.

    Also each completed OLAP stream is reported individually with its runtime.
    """
    def __init__(self, dsn, num_oltp_slots, num_olap_slots, csv_interval, ignored_queries = []):
        self.data = {}
        self.data['oltp'] = defaultdict(list)
        self.data['olap'] = [{
                'queries': {
                    query: {
                        'runtime': 0,
                        'status': 'IGNORED' if is_ignored_query(ignored_queries, query) else 'Waiting'
                        } for query in QUERY_IDS
                    },
                'ok_count': 0,
                'timeout_count': 0,
                'error_count': 0,
                'ignored_count': 0
        } for _ in range(num_olap_slots)]
        self.data['olap_stream'] = []
        self.uuid = uuid4()
        self.num_oltp_slots = num_oltp_slots
        self.updates = 0
        self.csv_interval = csv_interval
        self.dsn = dsn
        self.database = urlparse(self.dsn).path[1:]
        self.csv_olap = None
        self.csv_oltp = None
        self.csv_dbstats = None
        self.conn = None
        
    def _update_oltp_stats(self, stats):
        for stat in stats:
            self.data['oltp'][stat['query']].append(stat)

    def cleanup_oltp_stats(self, time_now):
        cutoff = int(time_now - HISTORY_LENGTH)
        for query_type in self.data['oltp'].keys():
            # keep HISTORY_LENGTH of history
            self.data['oltp'][query_type] = [s for s in self.data['oltp'][query_type] if s['timestamp'] >= cutoff]

    def _update_olap_stats(self, stats):
        stream_id = stats['stream']
        query_id = stats['query']

        if stats['status'] == 'Waiting' or stats['status'] == 'Running' or stats['status'] == 'IGNORED':
            # only overwrite status so we keep the last runtime
            self.data['olap'][stream_id]['queries'][query_id]['last-status'] = \
                self.data['olap'][stream_id]['queries'][query_id]['status']
            self.data['olap'][stream_id]['queries'][query_id]['status'] = stats['status']
            return

        self.data['olap'][stream_id]['queries'][query_id] = stats
        if self.csv_olap:
            self.csv_olap.write(f'{stream_id}, {query_id}, {stats["status"]}, {stats["runtime"]}\n')
            self.csv_olap.flush()
        status_count = stats['status'].lower() + '_count'
        self.data['olap'][stream_id][status_count] += 1

    def _update_olap_stream_stats(self, stats):
        self.data['olap_stream'].append(stats)

    def _process_queue(self, src, item):
        if src == 'oltp':
            self._update_oltp_stats(item)
        elif src == 'olap':
            self._update_olap_stats(item)
        elif src == 'olap_stream':
            self._update_olap_stream_stats(item)

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
        if not self.csv_oltp:
            self.csv_oltp = open('results/oltp.csv', 'w')
        if not self.csv_olap:
            self.csv_olap = open('results/olap.csv', 'w')
        if not self.csv_dbstats:
            self.csv_dbstats = open('results/dbstats.csv', 'w')
        if not self.conn:
            self.conn = DBConn(self.dsn, use_dict_cursor = True)
            self._update_cached_stats()

        self.cleanup_oltp_stats(time.time())

        self.updates += 1
        if self.updates % 10 == 0:
            self._update_cached_stats()
        if self.csv_interval and self.updates % self.csv_interval == 0:
            self.write_oltp_stats()
            self.write_dbstats()

    def olap_stats_for_stream_id(self, stream_id):
        return self.data['olap'][stream_id]

    def filter_last_1s(self, stats):
        ts = max(s['timestamp'] for s in stats) - 1
        return [s for s in stats if s['timestamp'] >= ts]

    def _oltp_tps(self, stats):
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

    def _oltp_latency(self, stats):
        if len(stats) == 0:
            return (0, 0, 0, 0)

        lat = [math.ceil(s['runtime']*1000) for s in stats]
        lat_1s = [math.ceil(s['runtime']*1000) for s in self.filter_last_1s(stats)]
        return (int(sum(lat_1s)/len(lat_1s)),
                min(lat),
                int(sum(lat)/len(lat)),
                max(lat))

    def oltp_total(self, query_type = None):
        data = self.data['oltp']
        # If query_type is None, then flatten all records into a single list, otherwise just take the records of the
        # requested type
        stats = data[query_type] if query_type != None else [item for sublist in data.values() for item in sublist]
        ok, err = [], []
        for s in stats:
            (ok if s['status'] == 'ok'  else err).append(s)
        tps = self._oltp_tps(ok)
        latency = self._oltp_latency(ok)
        return ((len(stats), len(ok), len(err)), tps, latency)


    def olap_totals(self):
        return tuple(sum(slot[slot_type] for slot in self.data['olap'])
                for slot_type in ['ok_count', 'error_count', 'timeout_count'])

    def olap_stream_totals(self):
        stats = self.data['olap_stream']
        completed_iterations = len(stats)
        if completed_iterations == 0:
            return ('-', 0)
        avg_runtime_it = sum(int(s['runtime']) for s in stats) // completed_iterations
        return (avg_runtime_it, completed_iterations)

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

    def write_oltp_stats(self):
        for query_type in QUERY_TYPES:
            counters, tps, latency = self.oltp_total(query_type)
            row = [datetime.now(), query_type, *counters, *tps, *latency]
            self.csv_oltp.write(', '.join(map(str, row)) + "\n")
        self.csv_oltp.flush()

    def write_dbstats(self):
        for row in self.columnstore_stats():
            self.csv_dbstats.write(', '.join(map(str, row)) + "\n")
        self.csv_dbstats.flush()

    # TODO rework this summary, the output summary is kind of useless, probably should be more inline with the stdout display
    def write_summary(self, csv_file, elapsed):
        row_nr = 0
        with open(csv_file, 'w') as csv:
            csv.write(';stream_id;query_id;timestamp_start;timestamp_stop;runtime;status;correctness_check\n')
            # oltp stream = stream 0
            stream_id = 0
            fake_date = datetime.now()
            elapsed_seconds = elapsed.total_seconds()
            for query_type in QUERY_TYPES:
                counters, tps, latency = self.oltp_total(query_type)
                total, ok , err = counters
                csv.write(f'{row_nr};{stream_id};{query_type}_total;{fake_date};{fake_date};{total};OK;OK\n')
                row_nr += 1
                csv.write(f'{row_nr};{stream_id};{query_type}_tps;{fake_date};{fake_date};{tps};OK;OK\n')
                row_nr += 1
                csv.write(f'{row_nr};{stream_id};{query_type}_latency;{fake_date};{fake_date};{latency};OK;OK\n')
                row_nr += 1
 
            # now all olap streams
            for stream_idx, stream in enumerate(self.data['olap']):
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
