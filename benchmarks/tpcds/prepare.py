import os
from glob import glob
from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory, TableGroup


class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.TABLES = (
        TableGroup(
            "customer_address",
            "customer_demographics",
            "date_dim",
            "warehouse",
            "ship_mode",
            "time_dim",
            "reason",
            "income_band",
            "item",
            "store",
            "call_center",
            "customer",
            "web_site",
            "household_demographics",
            "web_page",
            "promotion",
            "catalog_page",
            "inventory",
            "web_sales",
            "web_returns",
            "catalog_sales",
            "catalog_returns",
            "store_sales",
            "store_returns",
        ),
    )

    PrepareBenchmarkFactory.SIZING_FACTORS = {
        "s64da": {
            10: 0.75,
            100: 0.85,
            1000: 0.75,
        },
        "psql": {
            10: 6.0,
            100: 3.5,
            1000: 3.0,
        },
    }

    PrepareBenchmarkFactory.CLUSTER_SPEC = {
        "time_dim": "t_time_sk",
        "item": "i_current_price",
        "catalog_sales": "cs_net_profit,cs_quantity",
    }

    def _stream_to_db(self, table):
        return (
            f"grep ^{table} | sed -r 's/^'{table}' (.*)/\\1/' | "
            f'psql {self.args.dsn} -c "COPY {table} FROM STDIN '
            f"WITH (FORMAT CSV, DELIMITER '|')\""
        )

    def get_copy_cmds(self, table):
        copy_cmd = self.psql_exec_cmd(
            f"COPY {table} FROM STDIN WITH (FORMAT CSV, DELIMITER '|')"
        )
        full_path = os.path.join(self.args.data_dir, f"{table}.*gz")
        return [f"gunzip -c {data_file} | {copy_cmd}" for data_file in glob(full_path)]

    def _ingest_task_impl(self, table, dbgen):
        task = f"{dbgen} | recode ISO-8859-1..UTF-8 | "
        if "_sales" in table:
            returns_table = table.split("_")[0] + "_returns"
            task += (
                f"tee >({self._stream_to_db(table)}) "
                f">({self._stream_to_db(returns_table)}) | "
                f'grep -vE "^({table}|{returns_table})" | cat'
            )  # Hide the dsdgen output from the loader output
        else:
            task += f"{self._stream_to_db(table)}"

        return task

    def get_ingest_tasks(self, table):
        if self.args.data_dir:
            return self.get_copy_cmds(table)

        if "_returns" in table:
            return []

        use_chunks = (
            self.args.chunks > 1
            and self.args.scale_factor >= 100
            and table in ("inventory", "web_sales", "catalog_sales", "store_sales")
        )
        dbgen = (
            f"./dsdgen -SCALE {self.args.scale_factor} -TABLE {table} "
            f"-RNGSEED 1 -TERMINATE N -FILTER Y"
        )

        tasks = []
        if use_chunks:
            for chunk in range(1, self.args.chunks + 1):
                dbgen_cmd = f"{dbgen} -PARALLEL {self.args.chunks} -CHILD {chunk}"
                task = self._ingest_task_impl(table, dbgen_cmd)
                tasks.append(task)
        else:
            tasks.append(self._ingest_task_impl(table, dbgen))

        return tasks
