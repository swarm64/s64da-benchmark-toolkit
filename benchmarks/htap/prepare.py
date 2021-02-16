import os

from glob import glob

from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory, TableGroup
from benchmarks.htap import htap_loader as loader


class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.PYTHON_LOADER = True
    PrepareBenchmarkFactory.DO_SHUFFLE = True

    PrepareBenchmarkFactory.TABLES = (TableGroup(
        'warehouse',
        'item',
        'region',
        'nation',
        'supplier'
    ),)
    PrepareBenchmarkFactory.TABLES_ANALYZE = (TableGroup(
        'district',
        'customer',
        'history',
        'orders',
        'new_orders',
        'order_line',
        'stock',
        'item',
        'warehouse',
    ),)

    def get_ingest_tasks(self, table):
        data_dir = self.args.data_dir
        if data_dir:
            raise ValueError("Cannot use data dir with htap as this doesn't work with the process executor")

        else:
            dsn = self.args.dsn
            start_date = self.args.start_date
            if table in ['item', 'region', 'nation', 'supplier']:
                func_name = 'load_{}'.format(table)
                func = getattr(loader, func_name)
                return [(func, dsn)]
            elif table == 'warehouse':
                warehouses = range(1, self.args.scale_factor + 1)
                return [(loader.load_warehouse, dsn, w_id, start_date)
                        for w_id in warehouses]

            raise ValueError(f'Unknown table {table}')
