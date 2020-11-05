# Summary

This toolkit provides methods to execute the TPC-H, TPC-DS, and SSB benchmarks on:
- PostgreSQL
- EDB Postgres Advanced Server (EPAS)
- PostgreSQL with Swarm64 DA
- EPAS with Swarm64 DA

Important note: in order to guarantee compatibility between S64 DA and
s64da-benchmark-toolkit, please check out the Git tag that corresponds to your
version of S64 DA. For example, if your version of S64 DA is 5.1.0, clone this
repository and run `git checkout v5.1.0` in the the repository’s root folder
before proceeding. For S64 DA versions 4.0.0 and below checkout v4.0.0_and_below.

# Prerequisites

- Python min. 3.6 and pip3
- For TPC-DS only: Linux package `recode`
- Install additional packages, for Python 3.6 eg. with:
  `/usr/bin/python3.6 -m pip install -r requirements.txt`
- The `psql` PostgreSQL client
- For loading the data, the database must be accessible with the user
  `postgres` or `enterprisedb` *without password*


# Creating a database and loading data

Load a database with a dataset. If the database does not exist, it will be
created. If it does exist, it will be deleted and recreated.

    ./prepare_benchmark \
        --dsn postgresql://postgres@localhost/<target-db> \
        --benchmark <tpch|tpcds|ssb> \
        --schema=<schema-to-deploy> \
        --scale-factor=<scale-factor-to-use>

For example in order to load tpch dataset using PostgreSQL with Swarm64 DA
performance schema:

    ./prepare_benchmark \
        --dsn=postgresql://postgres@localhost:5432/example-database \
        --benchmark=tpch \
        --schema=s64da_performance \
        --scale-factor=1000

### Required Parameters

Parameter      | Description
-------------- | -----------
`dsn`          | The full DSN of the DB to connect to. DSN layout: <pre>postgresql://&lt;user&gt;@&lt;host&gt;:&lt;target-port&gt;/&lt;target-db&gt;</pre> The port is optional and the default is 5432. Example with port 5444 and use of EPAS: <pre>--dsn postgresql://enterprisedb@localhost:5444/example-database</pre>
`benchmark`    | The benchmark to use: `tpch`, `tpcds` or `ssb`
`schema`       | The schema to deploy. Schemas are directories in the benchmarks/\<benchmark\>/schemas directory. See the table below for the supported schemas.
`scale-factor` | The scale factor to use, such as `10`, `100` or `1000`.

#### Schema Parameter Values

Value                   | Description
----------------------- | -----------
`psql_native`           | the standard PostgreSQL schema
`s64da_native`          | as above but with the S64 DA extension with its default feature set enabled
`s64da_native_enhanced` | as above, but with some of the S64 DA opt-in features enabled, such as `columnstore` index
`s64da_performance`     | schema that provides the best performance for S64 DA

### Optional Parameters

Parameter                      | Description
------------------------------ | -----------------------------------------------
`chunks`                       | Chunk large tables into smaller pieces during ingestion. Default: `10`
`max-jobs`                     | Limit the overall loading parallelism to this amount of jobs. Default: `8`
`check-diskspace-of-directory` | If flag is present, a disk space check on the passed storage directory will be performed prior to ingestion
`s64da-license-path`           | The path to the S64 DA license. Default: `/s64da.license`
`data-dir`                     | The directory holding the data files to ingest from. Default: none
`num-partitions`               | The number of partitions for partitioned schemas. Default: none

Depending on the scale factor you chose, it might take several hours for the
script to finish. After the script creates the database, it loads the data,
creates primary keys, foreign keys, and indices. Afterwards, it runs VACUUM
and ANALYZE.


# Runnning a benchmark

Start a benchmark:

    ./run_benchmark \
        --dsn postgresql://postgres@localhost/<target-db> \
        --benchmark <tpch|tpcds|ssb>

This runs the benchmark with a 15 minute runtime restriction per query. You can
use the `--timeout` parameter to adjust this limit.

### Required Parameters

Parameter   | Description
----------- | -----------------------------------------------
`dsn`       | The full DSN of the DB to connect to. DSN layout: <pre>postgresql://&lt;user&gt;@&lt;host&gt;:&lt;target-port&gt;/&lt;target-db&gt;</pre> The port is optional and the default is 5432. Example with port 5444 and use of EPAS: <pre>--dsn postgresql://enterprisedb@localhost:5444/example-database</pre>
`benchmark` | The benchmark to use: `tpch`, `tpcds` or `ssb`

Note: if you enable correctness checks with the `--check-correctness` flag, the
parameter `--scale-factor` is required.

### Optional Parameters

Parameter             | Description
--------------------- | -----------
`config`              | Path to additional YAML configuration file
`timeout`             | The maximum time a query may run, such as `30min`
`streams`             | The number of parallel query streams, can be used for throughput tests.
`steam-offset`        | With which stream to start if running multiple streams. Defaults: `1`
`netdata-output-file` | File to write Netdata stats to. Requires `netdata` key to be present in configuration. Default: none
`output`              | How the results should be formatted. Multiple options possible. Default: none
`csv-file`            | Path to the CSV file for output if `csv` output is selected. Default: `results.csv` in the current directory.
`check-correctness`   | Compares each query result with pre-recorded results and stores them in the `query_results` directory. Requires `scale-factor` to be set.
`scale-factor`        | Scale factor for the correctness comparison. Default: none
`explain-analyze`     | Whether to run EXPLAIN ANALYZE. Query plans will be saved into the `plans` directory.


# Test parameterization with additional YAML configuration

You can modify the existing configuration files located under the configs
directory. By default, the toolkit loads loads the respective `default.yaml`
configuration file for each benchmark.
Alternatively, you can create an additional configuration file to control
test execution more granularly. An example YAML file for the TPC-H benchmark
might look as follows:

    timeout: 30min
    ignore:
      - 18
      - 20
      - 21

    dbconfig:
      max_parallel_workers: 96
      max_parallel_workers_per_gather: 32

To use this file, pass the `--config=<path-to-file>` argument to the test
executor. In this example, the query timeout is set to `30min` and queries 18,
20, and 21 will not be run. Additionally, the database parameters
`max_parallel_workers` and `max_parallel_workers_per_gather` will be set to
`96` and `32`, respectively.

In order to perform changes to the database configuration, the user needs to
have superuser privileges. Any change to the database configuration is applied
to the whole database system before the benchmark starts. If any change was
applied manually, the whole database configuration will be reset to that in the
PostgreSQL configuration file after the benchmark completes.

Some options can be passed on the command line and in a config file.
Any such option passed on the command line will override the value set in the
config file.
