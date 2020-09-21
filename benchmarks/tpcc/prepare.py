
from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory, TableGroup
from benchmarks.tpcc.tpcc_loader import load_item, load_warehouse


class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.PYTHON_LOADER = True

    PrepareBenchmarkFactory.TABLES = (
        TableGroup('item'),
        TableGroup('warehouse')
    )
    PrepareBenchmarkFactory.TABLES_ANALYZE = (TableGroup(
        'district',
        'customer',
        'history',
        'orders',
        'new_orders',
        'order_line',
        'stock',
        'item'
    ),)

    def get_ingest_tasks(self, table):
        dsn = self.args.dsn
        if table == 'item':
            return [(load_item, dsn)]

        if table == 'warehouse':
            warehouses = range(1, self.args.scale_factor + 1)
            return [(load_warehouse, dsn, w_id) for w_id in warehouses]

        raise ValueError(f'Unknown table {table}')
