# Summary

This toolkit provides methods to execute the TPC-H, TPC-DS, and SSB benchmark on:
- PostgreSQL
- EDB Postgres Advanced Server (EPAS)
- PostgreSQL with Swarm64 DA
- EPAS with Swarm64 DA

Important notice: In order to guarantee compatibility between S64 DA and s64da-benchmark-toolkit, please checkout the GIT Tag that corresponds to your version of S64 DA. For example, if your version of S64 DA is 5.0.0-beta, clone this repository and execute git checkout v5.0.0-beta within the the repository root folder before proceeding. For S64 DA versions 4.0.0 and below checkout v4.0.0_and_below

# Prerequisites

- Python min. 3.6 and pip3
- For TPC-DS only: Linux package `recode`
- Install additional packages, for Python 3.6 eg. with: `/usr/bin/python3.6 -m pip install -r requirements.txt`
- The `psql` PostgreSQL client
- For loading the data, the database must be accessible with the user
  `postgres` or `enterprisedb` *without password*


# Create a database and load data

Load a database with a dataset. If the database does not exist, it will be created. If it does exist, it will be deleted and recreated.

    ./prepare_benchmark \
        --dsn postgresql://postgres@localhost/<target-db> \
        --benchmark <tpch|tpcds|ssb> \
        --schema=<schema-to-deploy> \
        --scale-factor=<scale-factor-to-use> \
  
  For example in order to load tpch dataset using PostgreSQL with Swarm64 DA performance schema:
```
    ./prepare_benchmark --dsn=postgresql://postgres@localhost:5432/example-database \
    --benchmark=tpch --scale-factor=1000 --schema=s64da_performance
```

### Required Parameters

   | Parameter      | Description                                            |
   | -------------- | ------------------------------------------------------ |
   | `dsn`          | The full DSN of the DB to connect to. DSN layout: <br> ``postgresql://<user>@<host>:<target-port>/<target-db>`` <br> The port is optional and the default is 5432.<br> Example with port 5444 and use of EPAS: ``--dsn postgresql://enterprisedb@localhost:5444/example-database``|
   | `benchmark`    | The benchmark to use: `tpch`, `tpcds` or `ssb`         |
   | `schema`       | The schema to deploy. Schemas are directories in the benchmarks/\<benchmark\>/schemas directory and each benchmark has the following schemas: <br> - `psql_native`: standard PostgreSQL schema;<br> - `s64da_native`: as `psql_native` but with S64 DA enabled and only its default features;<br> - `s64da_native_enhanced`: as `s6da_native`, but with using some S64 DA opt-in features, such as the columnstore index; <br> - `s64da_performance`: schema that provides the best performance for S64 DA. <br>The schema name equals the directory name. |
   | `scale-factor` | The scale factor to use, such as `10`, `100` or `1000`.|

### Optional Parameters

   | Parameter                      | Description                                          |
   | ----------------               | ---------------------------------------------------- |
   | `chunks`                       | Chunk large tables into smaller pieces during ingestion. Default: `10` |
   | `max-jobs`                     | Limit the overall loading parallelism to this amount of jobs. Default: `8` |
   | `check-diskspace-of-directory` | If flag is present, a disk space check on the passed storage directory will be performed prior to ingestion. |
   | `s64da-license-path`           | The path to the S64 DA license. Default: `/s64da.license` |

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
| `dsn`       | The full DSN of the DB to connect to. DSN layout: <br> ``postgresql://<user>@<host>:<target-port>/<target-db>`` <br> The port is optional and the default is 5432.<br> Example with port 5444 and use of EPAS: ``--dsn postgresql://enterprisedb@localhost:5444/example-database``|
| `benchmark` | The benchmark to use: `tpch`, `tpcds` or `ssb`  |

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
will change to `32`.

To perform changes to the database configuration, the user needs to have superuser
privileges. Any change to the database configuration is applied before
the benchmark starts to the whole system. If any change was applied, after the benchmark completes the whole database
configuration is reset to the PostgreSQL configuration files.
