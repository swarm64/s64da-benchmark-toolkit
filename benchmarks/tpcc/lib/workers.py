
import asyncio
import signal
import sys
import time

from datetime import datetime, timedelta

from multiprocessing import Array, Event, Queue
from multiprocessing.pool import Pool

import asyncpg

# from faster_fifo import Queue

from benchmarks.tpcc.lib.tpcc import TPCC
from benchmarks.tpcc.lib.queries import Queries
from benchmarks.tpcc.lib.monitor import Monitor

from s64da_benchmark_toolkit.dbconn import DBConn

reporting_queue = Queue()
timestamps = Array('f', [0.0] * 1000)

def tpcc_worker(worker_id, args):
    increment = timedelta(seconds=86400.0 /
        (args.orders_per_day * (1 / 0.44)) * args.oltp_workers)
    tpcc = TPCC(worker_id, args.scale_factor, args.start_date, increment)

    with DBConn(args.dsn) as conn:
        while True:
            tpcc.next_transaction(conn, args.dummy_db)
            if (tpcc.ok_count + tpcc.err_count + 1) % 100 == 0:
                reporting_queue.put_nowait(('tpcc', tpcc.stats(worker_id)))
                timestamps[worker_id] = tpcc.timestamp.timestamp()

def tpch_worker(worker_id, args):
    queries = Queries(args.dsn, worker_id)
    while True:
        max_timestamp = max(timestamps)
        date = str(datetime.fromtimestamp(max_timestamp).date())
        queries.run_next_query(date, reporting_queue)

def run_impl(args):
    def on_error(what):
        print(f'Caught error: {what}')
        sys.exit(1)

    try:
        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

        total_workers = args.oltp_workers + args.olap_workers + 1
        with Pool(processes=total_workers) as pool:
            signal.signal(signal.SIGINT, original_sigint_handler)
            for tpcc_worker_id in range(args.oltp_workers):
                pool.apply_async(tpcc_worker, (tpcc_worker_id, args),
                                 error_callback=on_error)

            for tpch_worker_id in range(args.olap_workers):
                pool.apply_async(tpch_worker, (tpch_worker_id, args),
                                 error_callback=on_error)

            monitor = Monitor(args)
            monitor.display(reporting_queue, timestamps)

    except KeyboardInterrupt:
        pool.terminate()
