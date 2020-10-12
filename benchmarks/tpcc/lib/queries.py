
import time

from itertools import cycle

import yaml

from s64da_benchmark_toolkit.dbconn import DBConn
from .helpers import Random

QUERIES = {
    1:  "SELECT * FROM pricing_summary('__DATE__');",
    3:  "SELECT * FROM shipping_priority('__DATE__');",
    4:  "SELECT * FROM priority_checking('__DATE__');",
    5:  "SELECT * FROM local_supplier_volume('__DATE__');",
    6:  "SELECT * FROM forecasting_revenue_change('__DATE__', 0.1);",
    7:  "SELECT * FROM volume_shipping('__DATE__', '__WAREHOUSE__', '__WAREHOUSE__')",
    8:  "SELECT * FROM market_share('__DATE__', '__WAREHOUSE__', '__ITEM__');",
    9:  "SELECT * FROM product_type_profit_measure('__ITEM_PART__')",
    13: "SELECT * FROM customers_distribution();",
    14: "SELECT * FROM promotion_effect('__DATE__', '__ITEM__');",
    15: "SELECT * FROM top_warehouse('__DATE__');",
    17: "SELECT * FROM small_quantity_order_revenue('__ITEM__');",
    # 18: "SELECT * FROM large_volume_customer();",
    20: "SELECT * FROM potential_part_promotion('__DATE__', '__ITEM__');",
    # 21: "SELECT * FROM waiting_orders();"
}


class Queries:
    def __init__(self, dsn, stream_id):
        self.random = Random(stream_id)
        self.stream_id = stream_id
        self.dsn = dsn
        with open('benchmarks/tpcc/queries/streams.yaml', 'r') as streams_file:
            sequence = yaml.load(streams_file.read(), Loader=yaml.FullLoader)[stream_id]
            self.next_query_it = cycle(sequence)

        with DBConn(dsn) as conn:
            conn.cursor.execute('SELECT i_name FROM item')
            result = conn.cursor.fetchall()
            self.items = [item[0] for item in result]

            conn.cursor.execute('SELECT w_name FROM warehouse')
            result = conn.cursor.fetchall()
            self.warehouses = [warehouse[0] for warehouse in result]

    def run_next_query(self, in_date, queries_queue):
        next_query = next(self.next_query_it)
        sql = QUERIES.get(next_query)
        if not sql:
            # queries_queue.put((next_query, 'Skipped'))
            return

        if '__DATE__' in sql:
            sql = sql.replace('__DATE__', in_date)

        if '__WAREHOUSE__' in sql:
            sql = sql.replace('__WAREHOUSE__', Random.from_list(self.warehouses))

        if '__ITEM__' in sql:
            sql = sql.replace('__ITEM__', Random.from_list(self.items))

        if '__ITEM_PART__' in sql:
            sql = sql.replace('__ITEM_PART__', Random.from_list(self.items).split('-')[1])

        try:
            queries_queue.put((self.stream_id, next_query, 'Running'))
            with DBConn(self.dsn) as conn:
                tstart = time.time()
                conn.cursor.execute(sql)
                tstop = time.time()
            queries_queue.put((self.stream_id, next_query, tstop - tstart))
            # print(f'Q{ next_query }: { tstop - tstart }s')
        except Exception as exc:
            print(f'!!! Error Q{next_query}: {exc}')
