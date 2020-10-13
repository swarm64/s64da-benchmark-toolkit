
from psycopg2.extras import execute_values

from s64da_benchmark_toolkit.dbconn import DBConn
from benchmarks.tpcc.lib.helpers import Random


MAXITEMS = 100000
DIST_PER_WARE = 10
CUST_PER_DIST = 3000 # Standard says: 30k customers total (for all districts)
NUM_ORDERS = 3000
STOCKS = 100000


class TPCCLoader():
    def __init__(self, dsn, w_id, start_date=None):
        self.dsn = dsn
        self.w_id = w_id
        self.random = Random(w_id)
        self.start_date = start_date

    def insert_data(self, table, buffer, columns=None):
        print(f'Loading { table }')
        with DBConn(self.dsn) as conn:
            if columns:
                sql = f'INSERT INTO {table} ({ ",".join(columns) }) VALUES %s'
            else:
                sql = f'INSERT INTO {table} VALUES %s'

            execute_values(conn.cursor, sql, buffer)

    def load_warehouse(self):
        self.insert_data('warehouse', [[
            self.w_id,
            Random.string(5, prefix='name-'),
            Random.string(10, prefix='street1-'),
            Random.string(10, prefix='street2-'),
            Random.string(10, prefix='city-'),
            Random.get_state(),
            Random.numstring(5, prefix='zip-'),
            Random.sample() * 0.2,
            300000,
        ],])

    def load_district(self):
        buffer = []
        for d_id in range(1, DIST_PER_WARE + 1):
            buffer.append([
                d_id,
                self.w_id,
                Random.string(5, prefix='name-'),
                Random.string(10, prefix='street1-'),
                Random.string(10, prefix='street2-'),
                Random.string(10, prefix='city-'),
                Random.get_state(),
                Random.numstring(5, prefix='zip-'),
                Random.sample() * 0.2,
                30000,
                3001,
            ])
        self.insert_data('district', buffer)

    def load_customer(self):
        buffer = []
        for d_id in range(1, DIST_PER_WARE + 1):
            for c_id in range(1, CUST_PER_DIST + 1):
                c_last = Random.lastname(c_id - 1) if c_id < 1000 else \
                         Random.lastname(self.random.nurand(255, 0, 999))

                buffer.append([
                    c_id,
                    d_id,
                    self.w_id,
                    Random.string(Random.randint_inclusive(2, 10), prefix='first-'),
                    'OE',
                    c_last,
                    Random.string(10, prefix='street1-'),
                    Random.string(10, prefix='street2-'),
                    Random.string(10, prefix='city-'),
                    Random.get_state(),
                    Random.numstring(5, prefix='zip-'),
                    Random.numstring(16),
                    self.start_date or 'NOW()',
                    'GC' if Random.randint_inclusive(1, 100) > 10 else 'BC',
                    50000,
                    Random.sample() * 0.5,
                    -10,
                    10,
                    1,
                    0,
                    Random.string(Random.randint_inclusive(300, 500))
                ])
        self.insert_data('customer', buffer)

    def load_history(self):
        buffer = []
        for d_id in range(1, DIST_PER_WARE + 1):
            for c_id in range(1, CUST_PER_DIST):
                buffer.append([
                    c_id,
                    d_id,
                    self.w_id,
                    d_id,
                    self.w_id,
                    self.start_date or 'NOW()',
                    10,
                    Random.string(Random.randint_inclusive(12, 24))
                ])
        self.insert_data('history', buffer, columns=(
            'h_c_id', 'h_c_d_id', 'h_c_w_id', 'h_d_id', 'h_w_id', 'h_date',
            'h_amount', 'h_data'))

    def load_stock(self):
        buffer = []
        for s_id in range(1, STOCKS + 1):
            buffer.append([
                s_id,
                self.w_id,
                Random.randint_inclusive(10, 100),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                Random.string(24),
                0,
                0,
                0,
                Random.string(Random.randint_inclusive(26, 50))
            ])
        self.insert_data('stock', buffer)

    def _load_orders_impl(self):
        c_ids = list(range(1, NUM_ORDERS + 1))
        Random.shuffle(c_ids)

        order_line_counts = []
        buffer = []
        for d_id in range(1, DIST_PER_WARE + 1):
            for o_id in range(1, NUM_ORDERS + 1):
                order_line_counts.append(Random.randint_inclusive(5, 15))
                buffer.append([
                    o_id,
                    d_id,
                    self.w_id,
                    c_ids[o_id - 1],
                    self.start_date or 'NOW()',
                    Random.randint_inclusive(1, 10) if o_id < 2101 else None,
                    order_line_counts[-1],
                    1
                ])
        self.insert_data('orders', buffer)

        with DBConn(self.dsn) as conn:
            conn.cursor.execute(f'''
                INSERT INTO new_orders(no_o_id, no_d_id, no_w_id)
                SELECT o_id, o_d_id, o_w_id
                FROM orders
                WHERE o_id > 2100 AND o_w_id = { self.w_id }''')

        return order_line_counts

    def _load_order_line(self, order_line_counts):
        buffer = []
        for d_id in range(1, DIST_PER_WARE + 1):
            for o_id in range(1, NUM_ORDERS):
                order_line_count = order_line_counts[(d_id - 1) * NUM_ORDERS + o_id - 1]
                for ol_id in range(1, order_line_count + 1):
                    buffer.append([
                        o_id,
                        d_id,
                        self.w_id,
                        ol_id,
                        Random.randint_inclusive(1, MAXITEMS),
                        self.w_id,
                        (self.start_date or 'NOW()') if o_id < 2101 else None,
                        5,
                        0 if o_id < 2101 else Random.sample() * 9999.99,
                        Random.string(24)
                    ])
        self.insert_data('order_line', buffer)

    def load_orders(self):
        order_line_counts = self._load_orders_impl()
        self._load_order_line(order_line_counts)


def load_warehouse(dsn, w_id, start_date):
    print(f'Loading warehouse { w_id }')
    loader = TPCCLoader(dsn, w_id, start_date=start_date)
    loader.load_customer()
    loader.load_district()
    loader.load_history()
    loader.load_orders()
    loader.load_stock()
    loader.load_warehouse()


def load_item(dsn):
    loader = TPCCLoader(dsn, 0)
    buffer = []
    for i_id in range(1, MAXITEMS + 1):
        i_im_id = Random.randint_inclusive(1, 10000)
        i_price = Random.sample() * 100 + 1

        i_name_suffix = Random.string(5)
        i_name = f'item-{i_im_id}-{i_price}-{i_name_suffix}'

        i_data_suffix = Random.string(5)
        i_data = f'data-{i_name}-{i_data_suffix}'

        buffer.append([
            i_id,
            i_im_id,
            i_name[0:24],
            i_price,
            i_data[0:50]
        ])
    loader.insert_data('item', buffer)
