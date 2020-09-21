from datetime import datetime
from io import StringIO, SEEK_SET

from psycopg2.extras import execute_values

from s64da_benchmark_toolkit.dbconn import DBConn

from benchmarks.htap.lib.helpers import (
        Random, TPCCText, TPCHText, NATIONS, REGIONS, TimestampGenerator
)

MAXITEMS = 100000
DIST_PER_WARE = 10
CUST_PER_DIST = 30000 // DIST_PER_WARE
NUM_ORDERS = 3000
STOCKS = 100000
NUM_SUPPLIERS = 10000
NUM_NATIONS = 62
NUM_REGIONS = 5


class TPCCLoader():
    def __init__(self, dsn, warehouse_id, scale_factor, start_date=None):
        self.dsn = dsn
        self.warehouse_id = warehouse_id
        self.random = Random(seed=warehouse_id)
        self.tpcc_text = TPCCText(self.random)
        self.tpch_text = TPCHText(self.random)
        self.start_date = start_date or datetime.now()
        self.timestamp_generator = TimestampGenerator(self.start_date, scale_factor, self.random)

    def insert_data(self, table, data):
        with DBConn(self.dsn) as conn:
            sql = f'INSERT INTO {table} VALUES %s'
            execute_values(conn.cursor, sql, data)

    def load_warehouse(self):
        print(f'Loading warehouse ({self.warehouse_id})')
        self.insert_data(
                'warehouse', [
                        [
                                self.warehouse_id,
                                self.tpcc_text.string(5, prefix='name-'),
                                self.tpcc_text.string(10, prefix='street1-'),
                                self.tpcc_text.string(10, prefix='street2-'),
                                self.tpcc_text.string(10, prefix='city-'),
                                self.tpcc_text.state(),
                                self.tpcc_text.numstring(5, prefix='zip-'),
                                self.random.sample() * 0.2,
                                300000,
                        ],
                ]
        )

    def load_district(self):
        print(f'Loading district ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for d_id in range(1, DIST_PER_WARE + 1):
                row = [
                        d_id,
                        self.warehouse_id,
                        self.tpcc_text.string(5, prefix='name-'),
                        self.tpcc_text.string(10, prefix='street1-'),
                        self.tpcc_text.string(10, prefix='street2-'),
                        self.tpcc_text.string(10, prefix='city-'),
                        self.tpcc_text.state(),
                        self.tpcc_text.numstring(5, prefix='zip-'),
                        self.random.sample() * 0.2,
                        30000,
                        3001,
                ]
                f.write('\t'.join([str(v) for v in row]) + '\n')

            f.seek(SEEK_SET)
            conn.cursor.copy_from(f, 'district', null='None')

    def load_customer(self):
        print(f'Loading customer ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            for d_id in range(1, DIST_PER_WARE + 1):
                f = StringIO()
                for c_id in range(1, CUST_PER_DIST + 1):
                    c_last = self.tpcc_text.lastname(c_id - 1) if c_id < 1000 else \
                            self.tpcc_text.lastname(self.random.nurand(255, 0, 999))

                    row = [
                            c_id, d_id, self.warehouse_id,
                            self.tpcc_text.string(
                                    self.random.randint_inclusive(2, 10), prefix='first-'
                            ), 'OE', c_last,
                            self.tpcc_text.string(10, prefix='street1-'),
                            self.tpcc_text.string(10, prefix='street2-'),
                            self.tpcc_text.string(10, prefix='city-'),
                            self.tpcc_text.state(),
                            self.tpcc_text.numstring(5, prefix='zip-'),
                            self.tpcc_text.numstring(16), self.start_date or 'NOW()',
                            'GC' if self.random.randint_inclusive(1, 100) > 10 else 'BC', 50000,
                            self.random.sample() * 0.5, -10, 10, 1, 0,
                            self.tpcc_text.string(self.random.randint_inclusive(300, 500))
                    ]
                    f.write('\t'.join([str(v) for v in row]) + '\n')

                f.seek(SEEK_SET)
                conn.cursor.copy_from(f, 'customer', null='None')

    def load_history(self):
        print(f'Loading history ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for d_id in range(1, DIST_PER_WARE + 1):
                for c_id in range(1, CUST_PER_DIST):
                    row = [
                            c_id, d_id, self.warehouse_id, d_id, self.warehouse_id, self.start_date
                            or 'NOW()', 10,
                            self.tpcc_text.string(self.random.randint_inclusive(12, 24))
                    ]
                    f.write('\t'.join([str(v) for v in row]) + '\n')

            f.seek(SEEK_SET)
            conn.cursor.copy_from(
                    f,
                    'history',
                    null='None',
                    columns=(
                            'h_c_id', 'h_c_d_id', 'h_c_w_id', 'h_d_id', 'h_w_id', 'h_date',
                            'h_amount', 'h_data'
                    )
            )

    def load_stock(self):
        print(f'Loading stock ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for s_id in range(1, STOCKS + 1):
                row = [
                        s_id, self.warehouse_id,
                        self.random.randint_inclusive(10, 100),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24),
                        self.tpcc_text.string(24), 0, 0, 0,
                        self.tpcc_text.string(self.random.randint_inclusive(26, 50))
                ]
                f.write('\t'.join([str(v) for v in row]) + '\n')

            f.seek(SEEK_SET)
            conn.cursor.copy_from(f, 'stock', null='None')

    def _load_orders_impl(self):
        print(f'Loading orders ({self.warehouse_id})')
        c_ids = list(range(1, NUM_ORDERS + 1))
        self.random.shuffle(c_ids)
        order_line_infos = []
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for d_id in range(1, DIST_PER_WARE + 1):
                for o_id in range(1, NUM_ORDERS + 1):
                    entry_date = self.timestamp_generator.next()
                    order_line_infos.append((self.random.randint_inclusive(5, 15), entry_date))
                    row = [
                            o_id, d_id, self.warehouse_id, c_ids[o_id - 1], entry_date,
                            self.random.randint_inclusive(1, 10) if o_id < 2101 else None,
                            order_line_infos[-1][0], 1
                    ]
                    line = '\t'.join([str(v) for v in row]) + '\n'
                    f.write(line)

            f.seek(SEEK_SET)
            conn.cursor.copy_from(f, 'orders', null='None')

        with DBConn(self.dsn) as conn:
            conn.cursor.execute(
                    f'''
                INSERT INTO new_orders(no_o_id, no_d_id, no_w_id)
                SELECT o_id, o_d_id, o_w_id
                FROM orders
                WHERE o_id > 2100 AND o_w_id = { self.warehouse_id }'''
            )

        return order_line_infos

    def load_item(self):
        print(f'Loading item ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for i_id in range(1, MAXITEMS + 1):
                i_im_id = self.random.randint_inclusive(1, 10000)
                i_price = self.random.sample() * 100 + 1

                i_name_suffix = self.tpcc_text.string(5)
                i_name = f'item-{i_im_id}-{i_price}-{i_name_suffix}'

                if self.random.decision(1 / 10):
                    i_data = self.tpcc_text.data_original(26, 50)
                else:
                    i_data = self.tpcc_text.data(26, 50)

                row = [i_id, i_im_id, i_name[0:24], i_price, i_data]
                line = '\t'.join([str(v) for v in row]) + '\n'
                f.write(line)

            f.seek(SEEK_SET)
            conn.cursor.copy_from(f, 'item', null='None')

    def _load_order_line(self, order_line_infos):
        print(f'Loading order_line ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            for d_id in range(1, DIST_PER_WARE + 1):
                f = StringIO()
                for o_id in range(1, NUM_ORDERS):
                    order_line_count, entry_date = order_line_infos[(d_id - 1) * NUM_ORDERS + o_id -
                                                                    1]
                    for ol_id in range(1, order_line_count + 1):
                        row = [
                                o_id, d_id, self.warehouse_id, ol_id,
                                self.random.randint_inclusive(1, MAXITEMS), self.warehouse_id,
                                entry_date if o_id < 2101 else None, 5,
                                0 if o_id < 2101 else self.random.sample() * 9999.99,
                                self.tpcc_text.string(24)
                        ]
                        f.write('\t'.join([str(v) for v in row]) + '\n')

                f.seek(SEEK_SET)
                conn.cursor.copy_from(f, 'order_line', null='None')

    def load_orders(self):
        order_line_infos = self._load_orders_impl()
        self._load_order_line(order_line_infos)

    def load_region(self):
        print(f'Loading region ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for i in range(NUM_REGIONS):
                region_key, name = REGIONS[i]
                row = [region_key, name, self.tpch_text.random_length_text(31, 115)]
                f.write('\t'.join([str(v) for v in row]) + '\n')

            f.seek(SEEK_SET)
            conn.cursor.copy_from(f, 'region', null='None')

    def load_nation(self):
        print(f'Loading nation ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for i in range(NUM_NATIONS):
                nation_key, name, region_key = NATIONS[i]
                row = [nation_key, name, region_key, self.tpch_text.random_length_text(31, 114)]
                f.write('\t'.join([str(v) for v in row]) + '\n')

            f.seek(SEEK_SET)
            conn.cursor.copy_from(f, 'nation', null='None')

    def load_supplier(self):
        print(f'Loading supplier ({self.warehouse_id})')
        with DBConn(self.dsn) as conn:
            f = StringIO()
            for su_id in range(NUM_SUPPLIERS):
                nation_key = ord(self.tpcc_text.alnumstring(1))
                if (su_id + 7) % 1893 == 0:
                    comment = self.tpch_text.random_customer_text(25, 100, 'Complaints')
                elif (su_id + 13) % 1893 == 0:
                    comment = self.tpch_text.random_customer_text(25, 100, 'Recommends')
                else:
                    comment = self.tpch_text.random_length_text(25, 100)

                row = [
                        su_id, 'supplier-{:09d}'.format(su_id),
                        self.tpcc_text.string(
                                self.random.randint_inclusive(2, 32), prefix='address-'
                        ), nation_key,
                        self.tpch_text.random_phone_number(su_id),
                        self.random.randint_inclusive(-99999, 999999) / 100, comment
                ]
                f.write('\t'.join([str(v) for v in row]) + '\n')

            f.seek(SEEK_SET)
            conn.cursor.copy_from(f, 'supplier', null='None')


def load_warehouse(dsn, warehouse_id, scale_factor, start_date):
    loader = TPCCLoader(dsn, warehouse_id, scale_factor, start_date)
    loader.load_customer()
    loader.load_district()
    loader.load_history()
    loader.load_orders()
    loader.load_stock()
    loader.load_warehouse()


def load_item(dsn, scale_factor):
    loader = TPCCLoader(dsn, 0, scale_factor)
    loader.load_item()


def load_region(dsn, scale_factor):
    loader = TPCCLoader(dsn, 0, scale_factor)
    loader.load_region()


def load_nation(dsn, scale_factor):
    loader = TPCCLoader(dsn, 0, scale_factor)
    loader.load_nation()


def load_supplier(dsn, scale_factor):
    loader = TPCCLoader(dsn, 0, scale_factor)
    loader.load_supplier()
