# Summary

This toolkit provides methods to execute the TPC-H, TPC-DS, and SSB benchmark on
Swarm64 DA and native PostgreSQL.


# Prerequisites

- Python min. 3.6 and pip3
- For TPC-DS only: Linux package `recode`
- Install additional packages with `pip3 install -r requirements.txt`
- For loading the data, the database must be accessible with the user
  `postgres` *without password*


# Create a database and load data

1. To load a database with a dataset, go to the correct benchmark directory:\
   For TPC-H: `cd schemas/tpch`\
   For TPC-DS: `cd schemas/tpcds`\
   For SSB: `cd schemas/ssb`

2. Run the `loader.sh` script with the following parameters:

    ./loader.sh \
        --schema=<schema-to-deploy> \
        --scale-factor=<scale-factor-to-use> \
        --dbname=<target-db>

### Required Parameters

   | Parameter      | Description                                            |
   | -------------- | ------------------------------------------------------ |
   | `schema`       | The schema to deploy. Schemas are directories in the current working directory and start with either `sdb_` or `psql_`. The schema name equals the directory name. |
   | `scale-factor` | The scale factor to use, such as `10`, `100` or `1000`.      |
   | `dbname`       | The name of the target database. If the database does not exist, it will be created. If it does exist, it will be deleted and recreated.    |

### Optional Parameters

   | Parameter       | Description                                           |
   | ---------------- | ---------------------------------------------------- |
   | `num-partitions` | The number of partitions to use, if applicable. Default: 32 |
   | `chunks`         | Chunk large tables into smaller pieces during ingestion. Default: 10 |
   | `db-host`        | Alternative host for the database. Default: localhost |
   | `db-port`        | Alternative port for the database. Default: 5432|

   Depending on the scale factor you choose, the time it takes for the script
   to finish might take up to several hours. After the script creates the
   database, it loads the data, creates primary keys, foreign keys, and
   indices. Afterwards, it runs VACUUM and ANALYZE.


# Run a benchmark

Start a benchmark:

    ./run_benchmark \
        --dsn postgresql://postgres@localhost/<target-db> \
        --benchmark <tpch|tpcds|ssb>

This runs the benchmark with a 15 minute per query runtime restriction. You can
use the `--timeout` parameter to adjust this limit.

### Required Parameters

| Parameter   | Description                                     |
| ----------- | ----------------------------------------------- |
| `dsn`       | The full DSN of the DB to connect to. DSN layout: <br> ``postgresql://<user>@<host>:<target-port>/<target-db>`` <br> The port is optional and the default is 5432.<br> Example with port 5433: ``--dsn postgresql://postgres@localhost:5433/example-database``|
| `benchmark` | The benchmark to use: `tpch`, `tpcds` or `ssb`        |

### Optional Parameters

| Parameter  | Description                                        |
| ---------- | -------------------------------------------------- |
| `config`   | Path to additional YAML configuration file.        |
| `timeout`  | The maximum time a query may run, such as `30min`. |


# Test parameterization with additional YAML configuration

You can modify the existing configuration files located under the configs
directory. Per default, the tests loads the respective `default.yaml` 
configuration file.
Alternatively, you can create an additional configuration file to control 
test execution more granularly. An example YAML file for the TPC-H benchmark 
is as follows:

    timeout: 30min
    ignore:
      - 18
      - 20
      - 21

    dbconfig:
      max_parallel_workers: 96
      max_parallel_workers_per_gather: 32

To use this file, pass the `--config=<path-to-file>` argument to the test
executor. In this example, the query timeout is set to `30min`. Queries 18, 20,
and 21 will not execute. Additionally, the database parameters
`max_parallel_workers` will change to 96 and `max_parallel_workers_per_gather`
will change to `32`. Any change to the database configuration is applied before
the benchmark starts and are reverted after the benchmark completes.
