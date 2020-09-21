#!/usr/bin/env python3

import argparse
import time

from multiprocessing.pool import Pool
from multiprocessing import Value, Lock

import psycopg2

from benchmarks.tpcc.helpers import Random
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

    def new_order(self, conn):
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

        sql = 'SELECT new_order(%s, %s, %s, %s, %s, %s, %s, %s)'
        args = (w_id, c_id, d_id, order_line_count, all_local, itemid, supware, qty)
        return TPCC.execute_sql(conn, sql, args)

    def payment(self, conn):
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

        sql = 'SELECT payment(%s, %s, %s, %s, %s, %s, %s, %s)'
        args = (w_id, d_id, c_d_id, c_id, c_w_id, h_amount, byname, c_last)
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

    def delivery(self, conn):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        o_carrier_id = self.random.randint_inclusive(1, 10)

        sql = 'SELECT * FROM delivery(%s, %s, %s)'
        args = (w_id, o_carrier_id, DIST_PER_WARE)
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
    args_to_parse.add_argument('--scale-factor', required=True, type=int)
    args_to_parse.add_argument('--workers', required=True, type=int)
    args = args_to_parse.parse_args()

    counter_ok = Value('i', 0)
    counter_err = Value('i', 0)
    lock = Lock()

    scale_factor = args.scale_factor
    def run(worker):
        tpcc = TPCC(worker, scale_factor)
        with DBConn('postgresql://postgres@localhost/tpcc_test') as conn:
            while True:
                trx_type = tpcc.random.randint_inclusive(1, 23)
                if trx_type <= 10:
                    tpcc.new_order(conn)
                elif trx_type <= 20:
                    tpcc.payment(conn)
                elif trx_type <= 21:
                    tpcc.order_status(conn)
                elif trx_type <= 22:
                    tpcc.delivery(conn)
                elif trx_type <= 23:
                    tpcc.stocklevel(conn)

                # tstart = time.time()
                success = tpcc.new_order(conn)
                with lock:
                    if success:
                        counter_ok.value += 1
                    else:
                        counter_err.value += 1

    try:
        N = args.workers
        with Pool(processes=N) as pool:
            pool.map_async(run, range(N))

            ok = 0
            err = 0
            while True:
                time.sleep(1)
                with lock:
                    ok = counter_ok.value
                    err = counter_err.value
                    counter_ok.value = 0
                    counter_err.value = 0

                print(f'OK: {ok} ERR: {err} TOTAL: {ok + err}')
    except KeyboardInterrupt:
        pass

