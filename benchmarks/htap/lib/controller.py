import os
import signal
import time
import traceback

from datetime import datetime, timedelta
from urllib.parse import urlparse
from multiprocessing import Manager, Pool, Value, Queue

from psycopg2.errors import DuplicateDatabase, DuplicateTable, ProgrammingError

from benchmarks.htap.lib.helpers import nullcontext
from benchmarks.htap.lib.monitoring import Monitor
from benchmarks.htap.lib.stats import Stats
from benchmarks.htap.lib.analytical import AnalyticalStream
from benchmarks.htap.lib.transactional import TransactionalWorker

from s64da_benchmark_toolkit.dbconn import DBConn

class HTAPController:
    # have the shared-memory primitives static as otherwise the multiprocessing
    # inheritance scheme doesn't work. we want these primitives so we can use
    # "simple" synchronized primitives.
    latest_timestamp = Value('d', 0) # database record timestamp
    next_tsx_timestamp = Value('d', 0) # rtc time at which we can do the next oltp tsx
    stats_queue = Queue() # queue for communicating statistics

    def __init__(self, args):
        self.args = args
        self.next_tsx_timestamp.value = time.time()
        self.tsx_timestamp_increment = 1.0 / self.args.target_tps if self.args.target_tps is not None else 0
        self.num_warehouses = self._query_num_warehouses()
        self.range_delivery_date = self._query_range_delivery_date()

        # update the shared value to the actual last ingested timestamp
        self.latest_timestamp.value = self.range_delivery_date[1].timestamp()
        self.csv_interval = args.csv_interval if 'csv' in args.output else None
        self.stats = Stats(self.args.dsn, self.args.oltp_workers, self.args.olap_workers, self.csv_interval, self.args.ignored_queries)
        self.monitor = Monitor(
                self.stats, self.args.oltp_workers, self.args.olap_workers, self.num_warehouses,
                self.range_delivery_date[0]
        )

        print(f'Warehouses: {self.num_warehouses}')

    def oltp_sleep(self):
        with self.next_tsx_timestamp.get_lock():
            self.next_tsx_timestamp.value = self.next_tsx_timestamp.value + self.tsx_timestamp_increment
            sleep_until = self.next_tsx_timestamp.value
        time_now = time.time()
        if time_now < sleep_until:
            time.sleep(sleep_until - time_now)

    def oltp_worker(self, worker_id):
        # do NOT introduce timeouts for the oltp queries! this will make that
        # the workload gets inbalanaced and eventually the whole benchmark stalls
        with DBConn(self.args.dsn) as conn:
            oltp_worker = TransactionalWorker(worker_id, self.num_warehouses, self.latest_timestamp, conn, self.args.dry_run)
            next_reporting_time = time.time() + 0.1
            while True:
                self.oltp_sleep()
                oltp_worker.next_transaction()
                if next_reporting_time <= time.time():
                    # its beneficial to send in chunks so try to batch the stats by accumulating 0.1s of samples
                    self.stats_queue.put(('oltp', oltp_worker.stats()))
                    next_reporting_time += 0.1


    def olap_worker(self, worker_id):
        stream = AnalyticalStream(worker_id, self.args, self.range_delivery_date[0],
                          self.latest_timestamp, self.stats_queue)
        while True:
            stream.run_next_query()

    def analyze_worker(self):
        tables = ['customer', 'district', 'history', 'item', 'nation', 'new_orders',
                  'order_line', 'orders', 'region', 'stock', 'supplier', 'warehouse']
        with DBConn(self.args.dsn) as conn:
            for table in tables:
                conn.cursor.execute(f'ANALYZE {table}')
            time.sleep(60*10)

    def _sql_error(self, msg):
        import sys
        print(f'ERROR: {msg} Did you run `prepare_benchmark`?')
        sys.exit(-1)

    def _query_range_delivery_date(self):
        with DBConn(self.args.dsn) as conn:
            try:
                conn.cursor.execute('SELECT min(ol_delivery_d), max(ol_delivery_d) FROM order_line')
                return conn.cursor.fetchone()
            except ProgrammingError:
                self._sql_error('Could not query the latest delivery date.')

    def _query_num_warehouses(self):
        with DBConn(self.args.dsn) as conn:
            try:
                conn.cursor.execute('SELECT count(distinct(w_id)) from warehouse')
                return conn.cursor.fetchone()[0]
            except ProgrammingError:
                self._sql_error('Could not query number of warehouses.')

    def _prepare_stats_db(self):
        dsn_url = urlparse(self.args.stats_dsn)
        dbname = dsn_url.path[1:]

        with DBConn(f'{dsn_url.scheme}://{dsn_url.netloc}/postgres') as conn:
            try:
                conn.cursor.execute(f"CREATE DATABASE {dbname} TEMPLATE template0 ENCODING 'UTF-8'")
            except DuplicateDatabase:
                pass

        with DBConn(self.args.stats_dsn) as conn:
            stats_schema_path = os.path.join('benchmarks', 'htap', 'stats_schema.sql')
            with open(stats_schema_path, 'r') as schema:
                schema_sql = schema.read()
                try:
                    conn.cursor.execute(schema_sql)
                except DuplicateTable:
                    pass

    def run(self):
        begin = datetime.now()
        elapsed = timedelta()

        if self.args.stats_dsn is not None:
            print(f"Statistics will be collected in '{self.args.stats_dsn}'.")
            self._prepare_stats_db()
            stats_conn_holder = DBConn(self.args.stats_dsn)
        else:
            print(f'Database statistics collection is disabled.')
            stats_conn_holder = nullcontext()

        def worker_init():
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        num_total_workers = self.args.oltp_workers + self.args.olap_workers + 1
        with stats_conn_holder as stats_conn:
            with Pool(num_total_workers, worker_init) as pool:
                oltp_workers = pool.map_async(self.oltp_worker, range(self.args.oltp_workers))
                olap_workers = pool.map_async(self.olap_worker, range(self.args.olap_workers))
                analyze_worker = pool.apply_async(self.analyze_worker)

                try:
                    update_interval = timedelta(seconds=min(self.args.monitoring_interval, self.args.csv_interval))
                    display_interval = timedelta(seconds=self.args.monitoring_interval)
                    next_display = datetime.now() + display_interval
                    next_update  = datetime.now() + update_interval
                    while True:
                        # the workers are not supposed to ever stop.
                        # so test for errors by testing for ready() and if so propagate them
                        # by calling .get()
                        if self.args.oltp_workers > 0 and oltp_workers.ready():
                            oltp_workers.get()
                        if self.args.olap_workers > 0 and olap_workers.ready():
                            olap_workers.get()
                        if analyze_worker.ready():
                            analyze_worker.get()

                        while datetime.now() < next_update:
                            self.stats.process_queue(self.stats_queue)
                            time.sleep(0.1)

                        time_now = datetime.now()
                        elapsed = time_now - begin
                        if elapsed.total_seconds() >= self.args.duration:
                            break

                        self.stats.update()
                        next_update = time_now + update_interval
                        if 'print' in self.args.output and next_display <= time_now:
                            next_display += display_interval
                            self.monitor.update_display(elapsed.total_seconds(), time_now, stats_conn,
                                datetime.fromtimestamp(self.latest_timestamp.value))
                except KeyboardInterrupt:
                    pass
                finally:
                    self.monitor.display_summary(elapsed)
                    self.stats.write_summary(self.args.csv_file, elapsed)
