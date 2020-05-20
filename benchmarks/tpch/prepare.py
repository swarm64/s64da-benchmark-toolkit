
from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory, TableGroup

class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.TABLES = (TableGroup(
        'region',
        'customer',
        'lineitem',
        'nation',
        'orders',
        'part',
        'partsupp',
        'supplier'
    ),)

    PrepareBenchmarkFactory.SIZING_FACTORS = {
        's64da': {
            1: 1.0,
            10: 1.0,
            100: 1.0,
            300: 0.75,
            1000: 0.75,
            3000: 0.75
        },
        'psql': {
            10: 3.0,
            100: 2.5,
            300: 2.5,
            1000: 2.5,
            3000: 2.5
        }
    }

    PrepareBenchmarkFactory.CLUSTER_SPEC = {
        'lineitem': 'l_shipdate,l_receiptdate',
        'orders': 'o_orderdate'
    }

    TABLE_CODES = {
        'region': 'r',
        'customer': 'c',
        'lineitem': 'L',
        'nation': 'n',
        'orders': 'O',
        'part': 'P',
        'partsupp': 'S',
        'supplier': 's'
    }

    def get_ingest_tasks(self, table):
        use_chunks = self.args.chunks > 1 and table not in ('nation', 'region')

        table_code = PrepareBenchmark.TABLE_CODES[table]

        dbgen_cmd = f'./dbgen -s {self.args.scale_factor} -T {table_code} -o'
        psql_copy = self.psql_exec_cmd(f"COPY {table} FROM STDIN WITH DELIMITER '|'")

        if use_chunks:
            return [f'{dbgen_cmd} -S {chunk} -C {self.args.chunks} 2>/dev/null | {psql_copy}' for
                    chunk in range(1, self.args.chunks + 1)]

        return [f'{dbgen_cmd} 2>/dev/null | {psql_copy}']
