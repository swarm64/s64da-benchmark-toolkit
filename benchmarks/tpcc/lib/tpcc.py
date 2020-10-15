
import psycopg2

from .helpers import Random

MAXITEMS = 100000
DIST_PER_WARE = 10
CUST_PER_DIST = 3000
NUM_ORDERS = 3000
STOCKS = 100000
NAMES = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']

class TPCC:
    def __init__(self, seed, scale_factor, initial_timestamp, increment):
        self.random = Random(seed)
        self.scale_factor = scale_factor
        self.timestamp = initial_timestamp
        self.increment = increment
        self.ok_count = 0
        self.err_count = 0

    def stats(self, worker_id):
        return {
            'worker_id': worker_id,
            'ok_count': self.ok_count,
            'err_count': self.err_count
        }

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

    def next_transaction(self, conn, dummy_db):
        inc_ts = False
        # Randomify the timestamp
        timestamp_to_use = self.timestamp + self.random.sample() * self.increment

        trx_type = self.random.randint_inclusive(1, 23)
        success = True
        if not dummy_db:
            if trx_type <= 10:
                success = self.new_order(conn, timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 20:
                success = self.payment(conn, timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 21:
                success = self.order_status(conn)
            elif trx_type <= 22:
                success = self.delivery(conn, timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 23:
                success = self.stocklevel(conn)

        if success and inc_ts:
            self.timestamp += self.increment

        if success:
            self.ok_count += 1
        else:
            self.err_count += 1
