import logging
import math
import os
import time

from collections import defaultdict, deque
from datetime import datetime
from psycopg2.extras import register_uuid
from urllib.parse import urlparse
from uuid import uuid4

from benchmarks.htap.lib.analytical import QUERY_IDS, is_ignored_query
from s64da_benchmark_toolkit.dbconn import DBConn

LOG = logging.getLogger()
register_uuid()

QUERY_TYPES = ['new_order', 'payment', 'order_status', 'delivery', 'stock_level']

class OLTPBucketStats:
    def __init__(self):
        self.ok_transactions = 0
        self.err_transactions = 0
        self.min_runtime = float('inf')
        self.max_runtime = 0
        self.acc_runtime = 0

    def add_sample(self, status, runtime):
        if status == 'ok':
            self.ok_transactions += 1
        else:
            self.err_transactions += 1

        runtime = runtime * 1000
        self.min_runtime = min(self.min_runtime, runtime)
        self.max_runtime = max(self.max_runtime, runtime)
        self.acc_runtime += runtime

    def get_runtimes(self):
        return self.min_runtime, self.max_runtime, self.acc_runtime

    def get_ok_transactions(self):
        return self.ok_transactions

    def get_total_transactions(self):
        return self.ok_transactions + self.err_transactions

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
    This is kept in a list where the last history_length seconds of aggregated values are stored.
    Additionally a set of counters per query type is kept for the whole execution, where the
    number of transactions that succeed and error out are accumulated.

    OLAP
    -----
    For OLAP the status of each individual query (Running, Ok, Error, Ignored
    Timeout) is put into the monitoring queue, regardless of monitoring interval.

    Also each completed OLAP stream is reported individually with its runtime.
    """
    def __init__(self, dsn, num_oltp_slots, num_olap_slots, csv_interval, ignored_queries = [], history_length = 600, initial_sec = int(time.time())):
        self.data = {}
        self.data['oltp'] = deque([(initial_sec, {k:OLTPBucketStats() for k in QUERY_TYPES})], maxlen = history_length)
        # Bind to the copy method so it can be pickled, otherwise we need to use a lambda that cannot be serialized
        self.data['oltp_counts'] = defaultdict(defaultdict(int).copy)
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
        self.csv_olap_stream = None
        self.csv_oltp = None
        self.csv_dbstats = None
        self.conn = None
        self.history_length = history_length

    def get_history_length(self):
        return len(self.data['oltp'])

    def _update_oltp_stats(self, stats):
        for stat in stats:
            self.data['oltp_counts'][stat['query']][stat['status']] += 1

            oltp = self.data['oltp']
            second = int(stat['timestamp'])
            if second < oltp[0][0]:
                # Too old sample, discard it
                LOG.warning('too old OLTP sample arrived, discarding it: ' + stat)
                continue
            if second > oltp[-1][0]:
                # Sample goes beyond the horizon, expand the bucket list to accomodate all these seconds
                # As the struct is a deque, older buckets in the left side are automatically removed when we go beyond the maximum history size
                base = oltp[-1][0]
                oltp.extend((base + i, {k:OLTPBucketStats() for k in QUERY_TYPES} ) for i in range(1, 1 + second - base))

            # Now it is garanteed that we have a bucket for the second of this sample
            bucket = oltp[second - oltp[0][0]]
            assert(bucket[0] == second)
            bucket[1][stat['query']].add_sample(stat['status'], stat['runtime'])

    def _update_olap_stats(self, stat):
        stream_id = stat['stream']
        query_id = stat['query']

        if stat['status'] == 'Waiting' or stat['status'] == 'Running' or stat['status'] == 'IGNORED':
            # only overwrite status so we keep the last runtime
            self.data['olap'][stream_id]['queries'][query_id]['last-status'] = \
                self.data['olap'][stream_id]['queries'][query_id]['status']
            self.data['olap'][stream_id]['queries'][query_id]['status'] = stat['status']
        else:
            self.data['olap'][stream_id]['queries'][query_id] = stat
            self.data['olap'][stream_id][stat['status'].lower() + '_count'] += 1
            self._write_olap_stat(stat)

    def _update_olap_stream_stats(self, stat):
        self.data['olap_stream'].append(stat)
        self._write_olap_stream_stat(stat)

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
                conn.cursor.execute("select DISTINCT table_name,relation_blocks,compressed_blocks,cache_pages_usable from swarm64da.stat_all_column_store_indexes")
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
        if not self.csv_olap_stream:
            self.csv_olap_stream = open('results/olap_stream.csv', 'w')
        if not self.csv_dbstats:
            self.csv_dbstats = open('results/dbstats.csv', 'w')
        if not self.conn:
            self.conn = DBConn(self.dsn, use_dict_cursor = True)
            self._update_cached_stats()

        self.updates += 1
        if self.updates % 10 == 0:
            self._update_cached_stats()
        if self.csv_interval and self.updates % self.csv_interval == 0:
            self._write_oltp_stats()
            self._write_dbstats()

    def olap_stats_for_stream_id(self, stream_id):
        return self.data['olap'][stream_id]

    def oltp_counts(self, query_type = None):
        data = self.data['oltp_counts']
        ok , err = 0, 0
        stats = [data[query_type]] if query_type != None else data.values()
        for s in stats:
            ok += s['ok']
            err += s['error']
        return (ok, err)

    def oltp_total(self, query_type = None):
        oltp = self.data['oltp']
        tps = []
        latency = []
        total_txs = 0
        total_acc_runtime = 0
        min_runtime = float('inf')
        max_runtime = 0
        avg_latency_last_bucket = 0
        for bucket in oltp:
            # If query_type is None, then aggregate the stats of all query types
            bucket_stats = [bucket[1][query_type]] if query_type != None else bucket[1].values()

            bucket_ok_txs = 0
            bucket_total_txs = 0
            bucket_total_acc_runtime = 0
            for s in bucket_stats:
                # For TPS:
                bucket_ok_txs += s.get_ok_transactions()

                # For latencies
                runtimes = s.get_runtimes()
                min_runtime = min(min_runtime, runtimes[0])
                max_runtime = max(max_runtime, runtimes[1])
                total_acc_runtime += runtimes[2]
                bucket_total_acc_runtime += runtimes[2]

                num_txs = s.get_total_transactions()
                total_txs += num_txs
                bucket_total_txs += num_txs

            tps.append(bucket_ok_txs)
            latency.append(int(bucket_total_acc_runtime/bucket_total_txs) if bucket_total_txs != 0 else 0)

        tps_res = (tps[-2] if len(tps) > 1 else 0, min(tps),int(sum(tps)/len(tps)),max(tps))
        latency_res = (latency[-2] if len(latency) > 1 else 0, int(min_runtime), int(total_acc_runtime/total_txs), int(max_runtime)) if total_txs!=0 else (0,0,0,0)

        return tps_res, latency_res

    def olap_totals(self):
        return tuple(sum(slot[slot_type] for slot in self.data['olap'])
                for slot_type in ['ok_count', 'error_count', 'timeout_count'])

    def olap_stream_totals(self):
        stats = self.data['olap_stream']
        completed_iterations = len(stats)
        if completed_iterations == 0:
            return ('-', 0)
        avg_runtime_it = sum(s['runtime'] for s in stats) / completed_iterations
        return (int(avg_runtime_it), completed_iterations)

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

    def _write_olap_stat(self, stat):
        if self.csv_olap:
            self.csv_olap.write(f'{datetime.now()}, {stat["stream"]}, {stat["iteration"]}, {stat["query"]}, {stat["status"]}, {stat["runtime"]:.2f}\n')
            self.csv_olap.flush()

    def _write_olap_stream_stat(self, stat):
        if self.csv_olap_stream:
            self.csv_olap_stream.write(f'{datetime.now()}, {stat["stream"]}, {stat["iteration"]}, {stat["runtime"]:.2f}\n')
            self.csv_olap_stream.flush()

    def _write_oltp_stats(self):
        for query_type in QUERY_TYPES + [None]:
            counters = self.oltp_counts(query_type)
            tps, latency = self.oltp_total(query_type)
            name = 'All types' if query_type == None else query_type
            row = [datetime.now(), name, counters[0] + counters[1] , *counters, *tps, *latency]
            self.csv_oltp.write(', '.join(map(str, row)) + "\n")
        self.csv_oltp.flush()

    def _write_dbstats(self):
        for row in self.columnstore_stats():
            self.csv_dbstats.write(f'{datetime.now()},'+ ', '.join(map(str, row)) + '\n')
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
                counters = self.oltp_counts(query_type)
                tps, latency = self.oltp_total(query_type)
                ok , err = counters
                csv.write(f'{row_nr};{stream_id};{query_type}_total;{fake_date};{fake_date};{ok + err};OK;OK\n')
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
