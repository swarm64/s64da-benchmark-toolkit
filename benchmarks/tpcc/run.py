#!/usr/bin/env python3

import argparse
import sys
import traceback

from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from multiprocessing import Value, Lock, Queue

from benchmarks.tpcc.lib.queries import Queries
from benchmarks.tpcc.lib.output import Output
from benchmarks.tpcc.lib.tpcc import TPCC
from s64da_benchmark_toolkit.dbconn import DBConn


if __name__ == '__main__':
    args_to_parse = argparse.ArgumentParser()
    args_to_parse.add_argument(
        '--scale-factor', required=True, type=int, help=(
        'Scale factor to use. Set to same as when preparing the DB.'))

    args_to_parse.add_argument(
        '--oltp-workers', required=True, type=int, help=(
        'How many workers to generate new orders.'))

    args_to_parse.add_argument(
        '--olap-workers', required=True, type=int, help=(
        'How many query streams to run in parallel'))

    args_to_parse.add_argument(
        '--olap-timestamp', type=datetime.fromisoformat, help=(
        'Timestamp to use for OLAP query when there is no OLTP'))

    args_to_parse.add_argument(
        '--start-date', required=True, type=datetime.fromisoformat, help=(
        'First date of new orders, should be one day after ingestion date.'))

    args_to_parse.add_argument(
        '--end-date', required=True, type=datetime.fromisoformat, help=(
        'Date until to run. Process will abort once this date is hit first.'))

    args_to_parse.add_argument(
        '--orders-per-day', required=True, type=int, help=(
        'How many orders per day to generate.'))

    args = args_to_parse.parse_args()

    counter_ok = Value('i', 0)
    counter_err = Value('i', 0)
    order_timestamp = Value('f', 0.0)
    queries_queue = Queue()
    increment = timedelta(seconds=86400.0 / (
        args.orders_per_day * (1 / 0.44)) * args.oltp_workers)
    lock = Lock()

    dsn = 'postgresql://postgres@localhost/tpcc_test'

    def run_oltp_worker(worker_id, scale_factor, start_timestamp):
        tpcc = TPCC(worker_id, scale_factor)
        timestamp = start_timestamp
        with DBConn(dsn) as conn:
            while True:
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

                with lock:
                    order_timestamp.value = timestamp.timestamp()
                    if success:
                        counter_ok.value += 1
                    else:
                        counter_err.value += 1

                if success and inc_ts:
                    timestamp += increment

    def run_queries(stream_id):
        queries = Queries(dsn, stream_id)
        while True:
            date = str(datetime.fromtimestamp(order_timestamp.value).date())
            queries.run_next_query(date, queries_queue)

    def on_error(what):
        print(f'Error called: {what}')
        sys.exit(1)

    try:
        oltp_worker_args = [
            (worker_id, args.scale_factor, args.start_date) for worker_id
            in range(args.oltp_workers)]

        olap_worker_args = [worker_id for worker_id in range(args.olap_workers)]

        output = Output('postgresql://postgres@localhost/htap_stats')

        total_workers = args.oltp_workers + args.olap_workers + 1
        with Pool(processes=total_workers) as pool:
            if args.oltp_workers > 0:
                pool.starmap_async(run_oltp_worker, oltp_worker_args, error_callback=on_error)
            else:
                with lock:
                    order_timestamp.value = args.olap_timestamp.timestamp()

            if args.olap_workers > 0:
                pool.map_async(run_queries, olap_worker_args, error_callback=on_error)

            # Will block until args.end_date is reached
            output.display(
                lock, counter_ok, counter_err, order_timestamp, queries_queue,
                args.end_date, args.oltp_workers)
    except KeyboardInterrupt:
        pass
