from datetime import datetime
from io import StringIO, SEEK_SET

from psycopg2.extras import execute_values

from s64da_benchmark_toolkit.dbconn import DBConn

from benchmarks.htap.lib.helpers import (
    Random,
    TPCCText,
    TPCHText,
    NATIONS,
    REGIONS,
    TimestampGenerator,
    StringIteratorIO,
    DIST_PER_WARE,
    CUST_PER_DIST,
    NUM_ORDERS,
    MAX_ITEMS,
    STOCKS,
    NUM_SUPPLIERS,
    NUM_NATIONS,
    NUM_REGIONS,
    FIRST_UNPROCESSED_O_ID,
    TPCH_DATE_RANGE,
)

COPY_SIZE = 16384


class TPCCLoader:
    def __init__(self, dsn, warehouse_id=0, start_date=None):
        self.dsn = dsn
        self.warehouse_id = warehouse_id
        self.random = Random(seed=warehouse_id)
        self.tpcc_text = TPCCText(self.random)
        self.tpch_text = TPCHText(self.random)
        self.start_date = start_date or datetime.now()
        self.timestamp_generator = TimestampGenerator(self.start_date, self.random)

        # calculate delivery date offset. we scale the delivery date to be at the end of the
        # date range, because we only deliver the first FIRST_UNPROCESSED_O_ID items, but when
        # we start running the benchmark we start at the end of the time range
        fraction_delivered = (FIRST_UNPROCESSED_O_ID - 1) / NUM_ORDERS
        self.delivery_offset = (TPCH_DATE_RANGE[1] - TPCH_DATE_RANGE[0]) * (
            1 - fraction_delivered
        )

    def insert_data(self, table, data):
        with DBConn(self.dsn) as conn:
            sql = f"INSERT INTO {table} VALUES %s"
            execute_values(conn.cursor, sql, data)

    def row_for_copy(self, row):
        return "\t".join([str(v) for v in row]) + "\n"

    def load_region(self):
        print("Loading regions")
        assert self.warehouse_id == 0
        with DBConn(self.dsn) as conn:
            for i in range(NUM_REGIONS):
                region_key, name = REGIONS[i]
                self.insert_data(
                    "region",
                    [[region_key, name, self.tpch_text.random_length_text(31, 115)]],
                )

    def load_nation(self):
        print("Loading nation")
        assert self.warehouse_id == 0
        with DBConn(self.dsn) as conn:
            for i in range(NUM_NATIONS):
                nation_key, name, region_key = NATIONS[i]
                self.insert_data(
                    "nation",
                    [
                        [
                            nation_key,
                            name,
                            region_key,
                            self.tpch_text.random_length_text(31, 114),
                        ]
                    ],
                )

    def load_warehouse(self):
        print(f"Loading warehouse ({self.warehouse_id})")
        self.insert_data(
            "warehouse",
            [
                [
                    self.warehouse_id,
                    self.tpcc_text.string(5, prefix="name-"),
                    self.tpcc_text.string(10, prefix="street1-"),
                    self.tpcc_text.string(10, prefix="street2-"),
                    self.tpcc_text.string(10, prefix="city-"),
                    self.tpcc_text.state(),
                    self.tpcc_text.numstring(5, prefix="zip-"),
                    self.random.sample() * 0.2,
                    300000,
                ],
            ],
        )

    def generate_district(self, d_id):
        return self.row_for_copy(
            [
                d_id,
                self.warehouse_id,
                self.tpcc_text.string(5, prefix="name-"),
                self.tpcc_text.string(10, prefix="street1-"),
                self.tpcc_text.string(10, prefix="street2-"),
                self.tpcc_text.string(10, prefix="city-"),
                self.tpcc_text.state(),
                self.tpcc_text.numstring(5, prefix="zip-"),
                self.random.sample() * 0.2,
                30000,
                NUM_ORDERS + 1,
            ]
        )

    def load_district(self):
        print(f"Loading district ({self.warehouse_id})")
        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (self.generate_district(d_id) for d_id in range(1, DIST_PER_WARE + 1))
            )
            conn.cursor.copy_from(it, "district", null="None", size=COPY_SIZE)

    def generate_customer(self, d_id, c_id):
        c_last = (
            self.tpcc_text.lastname(c_id - 1)
            if c_id < 1000
            else self.tpcc_text.lastname(self.random.nurand(255, 0, 999))
        )

        state = self.tpcc_text.state()

        return self.row_for_copy(
            [
                c_id,
                d_id,
                self.warehouse_id,
                ord(state[0]),
                self.tpcc_text.string(
                    self.random.randint_inclusive(2, 10), prefix="first-"
                ),
                "OE",
                c_last,
                self.tpcc_text.string(10, prefix="street1-"),
                self.tpcc_text.string(10, prefix="street2-"),
                self.tpcc_text.string(10, prefix="city-"),
                state,
                self.tpcc_text.numstring(5, prefix="zip-"),
                self.tpcc_text.numstring(16),
                self.start_date,
                "GC" if self.random.randint_inclusive(1, 100) > 10 else "BC",
                50000,
                self.random.sample() * 0.5,
                -10,
                10,
                1,
                0,
                self.tpcc_text.string(self.random.randint_inclusive(300, 500)),
            ]
        )

    def load_customer(self):
        print(f"Loading customer ({self.warehouse_id})")
        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (
                    self.generate_customer(d_id, c_id)
                    for d_id in range(1, DIST_PER_WARE + 1)
                    for c_id in range(1, CUST_PER_DIST + 1)
                )
            )
            conn.cursor.copy_from(it, "customer", null="None", size=COPY_SIZE)

    def generate_history(self, d_id, c_id):
        return self.row_for_copy(
            [
                c_id,
                d_id,
                self.warehouse_id,
                d_id,
                self.warehouse_id,
                self.start_date,
                10,
                self.tpcc_text.string(self.random.randint_inclusive(12, 24)),
            ]
        )

    def load_history(self):
        print(f"Loading history ({self.warehouse_id})")
        copy_columns = (
            "h_c_id",
            "h_c_d_id",
            "h_c_w_id",
            "h_d_id",
            "h_w_id",
            "h_date",
            "h_amount",
            "h_data",
        )

        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (
                    self.generate_history(d_id, c_id)
                    for d_id in range(1, DIST_PER_WARE + 1)
                    for c_id in range(1, CUST_PER_DIST + 1)
                )
            )
            conn.cursor.copy_from(
                it, "history", null="None", columns=copy_columns, size=COPY_SIZE
            )

    def generate_stock(self, s_id):
        return self.row_for_copy(
            [
                s_id,
                self.warehouse_id,
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
                self.tpcc_text.string(24),
                0,
                0,
                0,
                self.tpcc_text.string(self.random.randint_inclusive(26, 50)),
            ]
        )

    def load_stock(self):
        print(f"Loading stock ({self.warehouse_id})")
        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (self.generate_stock(s_id) for s_id in range(1, STOCKS + 1))
            )
            conn.cursor.copy_from(it, "stock", null="None", size=COPY_SIZE)

    def generate_order(self, d_id, o_id):
        entry_date = self.timestamp_generator.next()
        o_ol_cnt = self.random.randint_inclusive(5, 15)
        self.order_lines.append((d_id, o_id, o_ol_cnt, entry_date))

        return self.row_for_copy(
            [
                o_id,
                d_id,
                self.warehouse_id,
                self.c_ids[o_id - 1],
                entry_date,
                self.random.randint_inclusive(1, 10)
                if o_id < FIRST_UNPROCESSED_O_ID
                else None,
                o_ol_cnt,
                1,
            ]
        )

    def generate_order_lines(self, order_line):
        d_id, o_id, order_line_count, entry_date = order_line
        rows = ""
        for ol_id in range(1, order_line_count + 1):
            rows += self.row_for_copy(
                [
                    o_id,
                    d_id,
                    self.warehouse_id,
                    ol_id,
                    self.random.randint_inclusive(1, MAX_ITEMS),
                    self.warehouse_id,
                    (entry_date + self.delivery_offset)
                    if o_id < FIRST_UNPROCESSED_O_ID
                    else None,
                    5,
                    0
                    if o_id < FIRST_UNPROCESSED_O_ID
                    else self.random.sample() * 9999.99,
                    self.tpcc_text.string(24),
                ]
            )
        return rows

    def load_orders(self):
        print(f"Loading orders ({self.warehouse_id})")
        self.order_lines = []
        self.c_ids = list(range(1, NUM_ORDERS + 1))
        self.random.shuffle(self.c_ids)

        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (
                    self.generate_order(d_id, o_id)
                    # generate in the order that a higher order number means a later transaction
                    for o_id in range(1, NUM_ORDERS + 1)
                    for d_id in range(1, DIST_PER_WARE + 1)
                )
            )
            conn.cursor.copy_from(it, "orders", null="None", size=COPY_SIZE)

        with DBConn(self.dsn) as conn:
            conn.cursor.execute(
                f"""
                INSERT INTO new_orders(no_o_id, no_d_id, no_w_id)
                SELECT o_id, o_d_id, o_w_id
                FROM orders
                WHERE o_id >= {FIRST_UNPROCESSED_O_ID} AND o_w_id = { self.warehouse_id }"""
            )

        print(f"Loading order_line ({self.warehouse_id})")
        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (
                    self.generate_order_lines(order_line)
                    for order_line in self.order_lines
                )
            )
            conn.cursor.copy_from(it, "order_line", null="None", size=COPY_SIZE)

    def generate_item(self, i_id):
        i_im_id = self.random.randint_inclusive(1, 10000)
        i_price = self.random.sample() * 100 + 1

        i_name_suffix = self.tpcc_text.string(5)
        i_name = f"item-{i_im_id}-{i_price}-{i_name_suffix}"

        if self.random.decision(1 / 10):
            i_data = self.tpcc_text.data_original(26, 50)
        else:
            i_data = self.tpcc_text.data(26, 50)

        return self.row_for_copy([i_id, i_im_id, i_name[0:24], i_price, i_data])

    def load_item(self):
        print(f"Loading items")
        assert self.warehouse_id == 0
        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (self.generate_item(i_id) for i_id in range(1, MAX_ITEMS + 1))
            )
            conn.cursor.copy_from(it, "item", null="None", size=COPY_SIZE)

    def generate_supplier(self, su_id):
        nation_key = ord(self.tpcc_text.alnumstring(1))
        if (su_id + 7) % 1893 == 0:
            comment = self.tpch_text.random_customer_text(25, 100, "Complaints")
        elif (su_id + 13) % 1893 == 0:
            comment = self.tpch_text.random_customer_text(25, 100, "Recommends")
        else:
            comment = self.tpch_text.random_length_text(25, 100)

        return self.row_for_copy(
            [
                su_id,
                "supplier-{:09d}".format(su_id),
                self.tpcc_text.string(
                    self.random.randint_inclusive(2, 32), prefix="address-"
                ),
                nation_key,
                self.tpch_text.random_phone_number(su_id),
                self.random.randint_inclusive(-99999, 999999) / 100,
                comment,
            ]
        )

    def load_supplier(self):
        print(f"Loading suppliers")
        assert self.warehouse_id == 0
        with DBConn(self.dsn) as conn:
            it = StringIteratorIO(
                (self.generate_supplier(su_id) for su_id in range(NUM_SUPPLIERS))
            )
            conn.cursor.copy_from(it, "supplier", null="None", size=COPY_SIZE)


def load_warehouse(dsn, warehouse_id, start_date):
    loader = TPCCLoader(dsn, warehouse_id, start_date)
    loader.load_customer()
    loader.load_district()
    loader.load_history()
    loader.load_orders()
    loader.load_stock()
    loader.load_warehouse()


def load_item(dsn):
    loader = TPCCLoader(dsn)
    loader.load_item()


def load_region(dsn):
    loader = TPCCLoader(dsn)
    loader.load_region()


def load_nation(dsn):
    loader = TPCCLoader(dsn)
    loader.load_nation()


def load_supplier(dsn):
    loader = TPCCLoader(dsn)
    loader.load_supplier()
