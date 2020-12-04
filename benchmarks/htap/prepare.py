import os

from glob import glob

from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory, TableGroup
from benchmarks.htap import htap_loader as loader


class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.PYTHON_LOADER = False
    PrepareBenchmarkFactory.DO_SHUFFLE = False

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

    def get_copy_cmds(self, table):
        copy_cmd = self.psql_exec_cmd(f'COPY {table} FROM STDIN CSV')
        if table in ('history', 'order_line', 'orders'):
            full_path = os.path.join(self.args.data_dir, f'{table}*.gz')
            return [f'gunzip -c {data_file} | {copy_cmd}' for data_file in glob(full_path)]
        else:
            data_file = os.path.join(self.args.data_dir, f'{table}.csv')
            return [f'cat {data_file} | {copy_cmd}']

    def get_ingest_tasks(self, table):
        data_dir = self.args.data_dir
        if data_dir:
            return self.get_copy_cmds(table)

        else:
            dsn = self.args.dsn
            start_date = self.args.start_date
            if table in ['item', 'region', 'nation', 'supplier']:
                func_name = 'load_{}'.format(table)
                func = getattr(loader, func_name)
                return [(func, dsn, self.args.scale_factor)]
            elif table == 'warehouse':
                warehouses = range(1, self.args.scale_factor + 1)
                return [(loader.load_warehouse, dsn, w_id, self.args.scale_factor, start_date)
                        for w_id in warehouses]

            raise ValueError(f'Unknown table {table}')
