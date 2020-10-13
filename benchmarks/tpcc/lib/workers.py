
import sys

from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from multiprocessing import Manager

from .queries import Queries
from .output import Output
from .tpcc import TPCC

from s64da_benchmark_toolkit.dbconn import DBConn


class Shared:
    def __init__(self):
        manager = Manager()
        self.counter_ok = manager.Value('i', 0)
        self.counter_err = manager.Value('i', 0)
        self.order_timestamp = manager.Value('f', 0.0)
        self.queries_queue = manager.Queue()
        self.lock = manager.Lock()
        self.stop = manager.Event()

def _oltp_worker(worker_id, dsn, scale_factor, start_timestamp, increment, shared):
    tpcc = TPCC(worker_id, scale_factor)
    timestamp = start_timestamp
    with DBConn(dsn) as conn:
        while not shared.stop.is_set():
            inc_ts = False
            # Randomify the timestamp for each worker
            timestamp_to_use = timestamp + tpcc.random.sample() * increment

            trx_type = tpcc.random.randint_inclusive(1, 23)
            if trx_type <= 10:
                success = tpcc.new_order(conn, timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 20:
                success = tpcc.payment(conn, timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 21:
                success = tpcc.order_status(conn)
            elif trx_type <= 22:
                success = tpcc.delivery(conn, timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 23:
                success = tpcc.stocklevel(conn)

            with shared.lock:
                shared.order_timestamp.value = timestamp.timestamp()
                if success:
                    shared.counter_ok.value += 1
                else:
                    shared.counter_err.value += 1

            if success and inc_ts:
                timestamp += increment

def _olap_worker(dsn, stream_id, shared):
    queries = Queries(dsn, stream_id)
    while not shared.stop.is_set():
        date = str(datetime.fromtimestamp(shared.order_timestamp.value).date())
        queries.run_next_query(date, shared.queries_queue)

def run_all(args, shared):
    def on_error(what):
        print(f'Error called: {what}')
        sys.exit(1)

    output = Output('postgresql://postgres@localhost/htap_stats')
    total_workers = args.oltp_workers + args.olap_workers + 1
    with Pool(processes=total_workers) as pool:
        if args.oltp_workers > 0:
            increment = timedelta(
                seconds=86400.0 / (args.orders_per_day * (1 / 0.44)) * args.oltp_workers)
            oltp_worker_args = [(worker_id, args.dsn, args.scale_factor,
                                 args.start_date, increment, shared) for worker_id
                                in range(args.oltp_workers)]
            pool.starmap_async(_oltp_worker, oltp_worker_args, error_callback=on_error)
        else:
            with shared.lock:
                order_timestamp.value = args.olap_timestamp.timestamp()

        if args.olap_workers > 0:
            olap_worker_args = [(args.dsn, worker_id, shared) for worker_id
                                in range(args.olap_workers)]
            pool.starmap_async(_olap_worker, olap_worker_args, error_callback=on_error)

        # Will block until args.end_date is reached
        output.display(shared, args.end_date, args.oltp_workers)
