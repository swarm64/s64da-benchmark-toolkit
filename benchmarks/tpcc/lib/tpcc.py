
import psycopg2

from .helpers import Random

MAXITEMS = 100000
DIST_PER_WARE = 10
CUST_PER_DIST = 3000
NUM_ORDERS = 3000
STOCKS = 100000
NAMES = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']

class TPCC:
    def __init__(self, seed, scale_factor, initial_timestamp, increment, conn):
        self.conn = conn
        self.random = Random(seed)
        self.scale_factor = scale_factor
        self.timestamp = initial_timestamp
        self.increment = increment
        self.ok_count = 0
        self.err_count = 0
        self.outstanding_deliveries = []

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

    def execute_sql(self, sql, args):
        try:
            self.conn.cursor.execute(sql, args)
            result = self.conn.cursor.fetchall()
        except psycopg2.errors.RaiseException as err:
            if 'Item record is null' in err.pgerror:
                return False
            raise

        except psycopg2.errors.DeadlockDetected as err:
            print(err)

        return True

    def new_order(self, timestamp='NOW()'):
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
        return self.execute_sql(sql, args)

    def payment(self, timestamp='NOW()'):
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
        return self.execute_sql(sql, args)

    def order_status(self):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
        c_last = self.random.lastname(self.random.nurand(255, 0, 999))
        byname = self.random.randint_inclusive(1, 100) <= 60

        sql = 'SELECT * FROM order_status(%s, %s, %s, %s, %s)'
        args = (w_id, d_id, c_id, c_last, byname)
        return self.execute_sql(sql, args)

    def delivery(self, timestamp='NOW()'):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        o_carrier_id = self.random.randint_inclusive(1, 10)

        sql = 'SELECT * FROM delivery(%s, %s, %s, %s)'
        args = (w_id, o_carrier_id, DIST_PER_WARE, timestamp)
        return self.execute_sql(sql, args)

    def stocklevel(self):
        w_id = self.random.randint_inclusive(1, self.scale_factor)
        d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
        level = self.random.randint_inclusive(10, 20)

        sql = 'SELECT * FROM stocklevel(%s, %s, %s)'
        args = (w_id, d_id, level)
        return self.execute_sql(sql, args)

    def next_transaction(self, timestamp, dummy_db):
        inc_ts = False
        # Randomify the timestamp
        # timestamp_to_use = self.timestamp + self.random.sample() * self.increment
        timestamp_to_use = timestamp

        trx_type = self.random.randint_inclusive(1, 23)
        if not dummy_db:
            if trx_type <= 10:
                success = self.new_order(timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 20:
                success = self.payment(timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 21:
                success = self.order_status()
            elif trx_type <= 22:
                success = self.delivery(timestamp=timestamp_to_use)
                inc_ts = True
            elif trx_type <= 23:
                success = self.stocklevel()
        else:
            success = True
            inc_ts = True

        if success:
            self.ok_count += 1
        else:
            self.err_count += 1

        return success and inc_ts
