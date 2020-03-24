
from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory

class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.TABLES = (
        'customer_address',
        'customer_demographics',
        'date_dim',
        'warehouse',
        'ship_mode',
        'time_dim',
        'reason',
        'income_band',
        'item',
        'store',
        'call_center',
        'customer',
        'web_site',
        'household_demographics',
        'web_page',
        'promotion',
        'catalog_page',
        'inventory',
        'web_sales',
        'catalog_sales',
        'store_sales',
    )

    def _stream_to_db(self, table):
        return (f"grep ^{table} | sed -r 's/^'{table}' (.*)/\\1/' | "
                f"psql {self.args.dsn} -c \"COPY {table} FROM STDIN "
                f"WITH (FORMAT CSV, DELIMITER '|')\"")

    def _ingest_task_impl(self, table, dbgen):
        task = f'{dbgen} | recode ISO-8859-1..UTF-8 | '
        if '_sales' in table:
            returns_table = table.split('_')[0] + '_returns'
            task += (f'tee >({self._stream_to_db(table)}) | '
                     f'({self._stream_to_db(returns_table)})')
        else:
            task += f'{self._stream_to_db(table)}'

        return task

    def get_ingest_tasks(self, table):
        use_chunks = (
            self.args.chunks > 1 and self.args.scale_factor >= 100 and
            table in ('inventory', 'web_sales', 'catalog_sales', 'store_sales')
        )
        dbgen = (f'./dsdgen -SCALE {self.args.scale_factor} -TABLE {table} '
                 f'-RNGSEED 1 -TERMINATE N -FILTER Y')

        tasks = []
        if use_chunks:
            for chunk in range(1, self.args.chunks + 1):
                dbgen_cmd = f'{dbgen} -PARALLEL {self.args.chunks} -CHILD {chunk}'
                task = self._ingest_task_impl(table, dbgen_cmd)
                tasks.append(task)
        else:
            tasks.append(self._ingest_task_impl(table, dbgen))

        return tasks
