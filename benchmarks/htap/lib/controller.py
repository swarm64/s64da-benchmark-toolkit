import os
import signal
import time
import traceback

from datetime import datetime, timedelta
from multiprocessing import Manager, Pool, Value
from urllib.parse import urlparse

from psycopg2.errors import DuplicateDatabase, DuplicateTable, ProgrammingError

from benchmarks.htap.lib.helpers import nullcontext
from benchmarks.htap.lib.monitoring import Stats, Monitor
from benchmarks.htap.lib.queries import Queries
from benchmarks.htap.lib.transactions import Transactions

from s64da_benchmark_toolkit.dbconn import DBConn


def oltp_worker(
        worker_id, dsn, scale_factor, dry_run, monitoring_interval, reporting_queue, date_value
):
    with DBConn(dsn) as conn:
        start_date = datetime.fromtimestamp(date_value.value)
        tpcc_worker = Transactions(worker_id, scale_factor, start_date, conn)
        reporting_interval = timedelta(seconds=monitoring_interval) / 2
        last_reporting_time = None
        while True:
            tpcc_worker.next_transaction(dry_run)
            now = datetime.now()
            if last_reporting_time is None or (now - last_reporting_time) >= reporting_interval:
                reporting_queue.put_nowait(('tpcc', tpcc_worker.stats(worker_id)))
                last_reporting_time = now


def olap_worker(worker_id, dsn, olap_timeout_seconds, dry_run, reporting_queue, date_value):
    with DBConn(dsn, statement_timeout=olap_timeout_seconds * 1000) as conn:
        queries = Queries(worker_id, conn)
        while True:
            max_date = datetime.fromtimestamp(date_value.value)
            queries.run_next_query(max_date, reporting_queue, dry_run)


class HTAPController:
    def __init__(self, args):
        self.dsn = args.dsn
        self.scale_factor = self._query_num_warehouses()
        self.dry_run = args.dry_run
        self.num_oltp_workers = args.oltp_workers
        self.num_olap_workers = args.olap_workers
        self.benchmark_duration = args.duration
        self.olap_query_timeout = args.olap_timeout
        self.monitoring_interval = args.monitoring_interval
        self.collect_stats = args.stats_dsn is not None
        self.stats_dsn = args.stats_dsn

        self.stats = Stats(self.num_oltp_workers, self.num_olap_workers)
        self.monitor = Monitor(
                self.stats, self.num_oltp_workers, self.num_olap_workers, self.scale_factor,
                self.monitoring_interval
        )

        print(f'Detected scale factor: {self.scale_factor}')

    def _sql_error(self, msg):
        import sys
        print(f'ERROR: {msg} Did you run `prepare_benchmark`?')
        sys.exit(-1)

    def _query_latest_delivery_date(self):
        with DBConn(self.dsn) as conn:
            try:
                conn.cursor.execute('SELECT max(ol_delivery_d) FROM order_line')
                return conn.cursor.fetchone()[0]
            except ProgrammingError:
                self._sql_error('Could not query the latest delivery date.')

    def _query_num_warehouses(self):
        with DBConn(self.dsn) as conn:
            try:
                conn.cursor.execute('SELECT count(distinct(w_id)) from warehouse')
                return conn.cursor.fetchone()[0]
            except ProgrammingError:
                self._sql_error('Could not query number of warehouses.')

    def _launch_workers(self, pool, queue, date_value):
        def worker_error(err):
            import sys
            print(f'Exception in worker: {err}')
            # FIXME: not all exceptions have __traceback__ set
            traceback.print_exception(type(err), err, err.__traceback__)
            sys.exit(1)

        for oltp_worker_id in range(self.num_oltp_workers):
            worker_args = (
                    oltp_worker_id, self.dsn, self.scale_factor, self.dry_run,
                    self.monitoring_interval, queue, date_value
            )
            pool.apply_async(oltp_worker, worker_args, error_callback=worker_error)

        for olap_worker_id in range(self.num_olap_workers):
            worker_args = (
                    olap_worker_id, self.dsn, self.olap_query_timeout, self.dry_run, queue,
                    date_value
            )
            pool.apply_async(olap_worker, worker_args, error_callback=worker_error)

    def _prepare_stats_db(self):
        dsn_url = urlparse(self.stats_dsn)
        dbname = dsn_url.path[1:]

        with DBConn(f'{dsn_url.scheme}://{dsn_url.netloc}/postgres') as conn:
            try:
                conn.cursor.execute(f"CREATE DATABASE {dbname} TEMPLATE template0 ENCODING 'UTF-8'")
            except DuplicateDatabase:
                pass

        with DBConn(self.stats_dsn) as conn:
            stats_schema_path = os.path.join('benchmarks', 'htap', 'stats_schema.sql')
            with open(stats_schema_path, 'r') as schema:
                schema_sql = schema.read()
                try:
                    conn.cursor.execute(schema_sql)
                except DuplicateTable:
                    pass

    def run(self):
        begin = datetime.now()
        latest_delivery_date = self._query_latest_delivery_date()

        if self.collect_stats:
            print(f"Statistics will be collected in '{self.stats_dsn}'.")
            self._prepare_stats_db()
            stats_conn_holder = DBConn(self.stats_dsn)
        else:
            print(f'Database statistics collection is disabled.')
            stats_conn_holder = nullcontext()

        def worker_init():
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        num_total_workers = self.num_oltp_workers + self.num_olap_workers
        with stats_conn_holder as stats_conn:
            with Pool(num_total_workers, worker_init) as pool:
                manager = Manager()
                queue = manager.Queue()
                shared_value = manager.Value('d', latest_delivery_date.timestamp())
                self.stats.set_latest_delivery_date(latest_delivery_date)
                self._launch_workers(pool, queue, shared_value)

                try:
                    while True:
                        time.sleep(self.monitoring_interval)
                        time_now = datetime.now()
                        elapsed = time_now - begin
                        self.stats.update(queue, time_now)
                        shared_value.value = self.stats.get_latest_delivery_date().timestamp()
                        self.monitor.update_display(elapsed.total_seconds(), time_now, stats_conn)
                        if (datetime.now() - begin).total_seconds() >= self.benchmark_duration:
                            break

                except KeyboardInterrupt:
                    pool.terminate()

                finally:
                    elapsed = datetime.now() - begin
                    self.monitor.display_summary(elapsed)
