
import os

from glob import glob

from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory, TableGroup
from benchmarks.tpcc.tpcc_loader import load_item, load_warehouse


class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.PYTHON_LOADER = False
    PrepareBenchmarkFactory.DO_SHUFFLE = False

    PrepareBenchmarkFactory.TABLES = (
        TableGroup('item'),
        TableGroup('warehouse')
    ) if PrepareBenchmarkFactory.PYTHON_LOADER else (
        TableGroup(
        'district',
        'customer',
    #    'history',
        'orders',
        'new_orders',
        'order_line',
        'stock',
        'item',
        'warehouse',
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
        if PrepareBenchmarkFactory.PYTHON_LOADER:
            dsn = self.args.dsn
            start_date = self.args.start_date
            if table == 'item':
                return [(load_item, dsn)]

            if table == 'warehouse':
                warehouses = range(1, self.args.scale_factor + 1)
                return [(load_warehouse, dsn, w_id, start_date) for w_id in warehouses]

            raise ValueError(f'Unknown table {table}')

        else:
            data_dir = self.args.data_dir
            assert data_dir, 'Non-Python loading selected, but no data dir present'
            return self.get_copy_cmds(table)

