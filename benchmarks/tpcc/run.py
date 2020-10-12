#!/usr/bin/env python3

import argparse
import sys

from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from multiprocessing import Value, Lock, Queue

import psycopg2

from benchmarks.tpcc.helpers import Random
from benchmarks.tpcc.queries import Queries
from benchmarks.tpcc.output import Output
from s64da_benchmark_toolkit.dbconn import DBConn

MAXITEMS = 100000
DIST_PER_WARE = 10
CUST_PER_DIST = 3000
NUM_ORDERS = 3000
STOCKS = 100000
NAMES = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']


class TPCC:
    def __init__(self, seed, scale_factor):
        self.random = Random(seed)
        self.scale_factor = scale_factor

    def other_ware(self, home_ware):
        if self.scale_factor == 1:
            return home_ware

        while True:
            tmp = self.random.randint_inclusive(1, self.scale_factor)
            if tmp == home_ware:
                return tmp

    @classmethod
    def execute_sql(cls, conn, sql, args):
        try:
            conn.cursor.execute(sql, args)
            result = conn.cursor.fetchall()
            # assert len(result) == order_line_count
        except psycopg2.Error:
            return False

        return True

    def new_order(self, conn, timestamp='NOW()'):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
        order_line_count = self.random.randint_inclusive(5, 15)
        rbk = self.random.randint_inclusive(1, 100)
        itemid = []
        supware = []
        qty = []
        all_local = 1

        for order_line in range(1, order_line_count + 1):
            itemid.append(self.random.nurand(8191, 1, MAXITEMS))
            if (order_line == order_line_count - 1) and (rbk == 1):
                itemid[-1] = -1

            if self.random.randint_inclusive(1, 100) != 1:
                supware.append(w_id)
            else:
                supware.append(self.other_ware(w_id))
                all_local = 0

            qty.append(self.random.randint_inclusive(1, 10))

        sql = 'SELECT new_order(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        args = (w_id, c_id, d_id, order_line_count, all_local, itemid, supware, qty, timestamp)
        return TPCC.execute_sql(conn, sql, args)

    def payment(self, conn, timestamp='NOW()'):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
        h_amount = self.random.randint_inclusive(1, 5000)
        c_last = self.random.lastname(self.random.nurand(255, 0, 999))

        byname = self.random.randint_inclusive(1, 100) <= 60
        if self.random.randint_inclusive(1, 100) <= 85:
            c_w_id = w_id
            c_d_id = d_id
        else:
            c_w_id = self.other_ware(w_id)
            c_d_id = self.random.randint_inclusive(1, DIST_PER_WARE)

        sql = 'SELECT payment(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        args = (w_id, d_id, c_d_id, c_id, c_w_id, h_amount, byname, c_last, timestamp)
        return TPCC.execute_sql(conn, sql, args)

    def order_status(self, conn):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
        c_last = self.random.lastname(self.random.nurand(255, 0, 999))
        byname = self.random.randint_inclusive(1, 100) <= 60

        sql = 'SELECT * FROM order_status(%s, %s, %s, %s, %s)'
        args = (w_id, d_id, c_id, c_last, byname)
        return TPCC.execute_sql(conn, sql, args)

    def delivery(self, conn, timestamp='NOW()'):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        o_carrier_id = self.random.randint_inclusive(1, 10)

        sql = 'SELECT * FROM delivery(%s, %s, %s, %s)'
        args = (w_id, o_carrier_id, DIST_PER_WARE, timestamp)
        return TPCC.execute_sql(conn, sql, args)

    def stocklevel(self, conn):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        level = self.random.randint_inclusive(10, 20)

        sql = 'SELECT * FROM stocklevel(%s, %s, %s)'
        args = (w_id, d_id, level)
        return TPCC.execute_sql(conn, sql, args)


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
