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


# Creating a Database and Loading Data

Load a database with a dataset. If the database does not exist, it will be
created. If it does exist, it will be deleted and recreated.

    ./prepare_benchmark \
        --dsn postgresql://postgres@localhost/<target-db> \
        --benchmark <tpch|tpcds|ssb|htap> \
        --schema=<schema-to-deploy> \
        --scale-factor=<scale-factor-to-use>

For example in order to load tpch dataset using PostgreSQL with Swarm64 DA
performance schema:

    ./prepare_benchmark \
        --dsn=postgresql://postgres@localhost:5432/example-database \
        --benchmark=tpch \
        --schema=s64da_performance \
        --scale-factor=1000

## Required Parameters

Parameter      | Description
-------------- | -----------
`dsn`          | The full DSN of the DB to connect to. DSN layout: <pre>postgresql://&lt;user&gt;@&lt;host&gt;:&lt;target-port&gt;/&lt;target-db&gt;</pre> The port is optional and the default is 5432. Example with port 5444 and use of EPAS: <pre>--dsn postgresql://enterprisedb@localhost:5444/example-database</pre>
`benchmark`    | The benchmark to use: `tpch`, `tpcds` or `ssb`
`schema`       | The schema to deploy. Schemas are directories in the benchmarks/\<benchmark\>/schemas directory. See the table below for the supported schemas.
`scale-factor` | The scale factor to use, such as `10`, `100` or `1000`.

### Schema Parameter Values

Value                         | Description
----------------------------- | -----------
`psql_native`                 | the standard PostgreSQL schema
`s64da_native`                | as above but with the S64 DA extension with its default feature set enabled
`s64da_native_enhanced`       | as above but with some of the S64 DA opt-in features enabled, such as `columnstore` index
`s64da_performance`           | schema that provides the best performance for S64 DA (includes removal of btree indexes, keys, and use of floating point)
`*_partitioned_id_hashed`     | schema like one of first four schemas but partitioning some tables using hash on main id column of the table
`*_partitioned_date_week`     | schema like one of first four schemas but partitioning tables with dates by weeks

## Optional Parameters

Parameter                      | Description
------------------------------ | -----------------------------------------------
`chunks`                       | Chunk large tables into smaller pieces during ingestion. Default: `10`
`max-jobs`                     | Limit the overall loading parallelism to this amount of jobs. Default: `8`
`check-diskspace-of-directory` | If flag is present, a disk space check on the passed storage directory will be performed prior to ingestion
`s64da-license-path`           | The path to the S64 DA license. Default: `/s64da.license`
`data-dir`                     | The directory holding the data files to ingest from. Default: none
`num-partitions`               | The number of partitions for partitioned schemas. Default: none
`start-date`                   | The data start date for HTAP benchmark

Depending on the scale factor you chose, it might take several hours for the
script to finish. After the script creates the database, it loads the data,
creates primary keys, foreign keys, and indices. Afterwards, it runs VACUUM
and ANALYZE.


# Runnning a Benchmark

Start a benchmark:

    ./run_benchmark \
        --dsn postgresql://postgres@localhost/<target-db> \
        [--benchmark] <tpch|tpcds|ssb|htap> \
        <optional benchmark-specific arguments>

This runs the benchmark with the default runtime restriction per query.
Some benchmarks support a `--timeout` parameter to adjust this limit.

Note: The `--benchmark` parameter has been deprecated and is ignored. The name of the benchmark
should directly follow the specification of `--dsn`.

## Required Parameters

Parameter   | Description
----------- | -----------------------------------------------
`dsn`       | The full DSN of the DB to connect to. DSN layout: <pre>postgresql://&lt;user&gt;@&lt;host&gt;:&lt;target-port&gt;/&lt;target-db&gt;</pre> The port is optional and the default is 5432. Example with port 5444 and use of EPAS: <pre>--dsn postgresql://enterprisedb@localhost:5444/example-database</pre>
&nbsp;      | Name of the the benchmark to use: `tpch`, `tpcds`, `ssb`, or `htap`

Note: if you enable correctness checks with the `--check-correctness` flag, the
parameter `--scale-factor` is required.

## Optional Parameters

The optional parameters differ by benchmark.
The ones for TPC-H, TPC-DS, and SSB are described in this section.
The parameters supported by HTAP are described in a separate section below.


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

# Test Parameterization with Additional YAML Configuration

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

Note: This feature is not supported by HTAP benchmark.


# HTAP Benchmark

A mixed workload benchmark implementation using a hybrid TPC-C/TPC-H schema is available in `benchmarks/htap`.
It draws inspiration from [sysbench-tpcc](https://github.com/Percona-Lab/sysbench-tpcc), [CHbenCHmark](https://db.in.tum.de/research/projects/CHbenCHmark/?lang=en), and [HTAPBench](https://github.com/faclc4/HTAPBench).

Data preparation is identical to the other benchmarks (see "Creating a database and loading data"
above).

The HTAP benchmark requires command line arguments that differ from the ones described above.
The `--dsn` argument is shared with the other benchmarks and must be provided.
The `--benchmark` argument is not used, instead the name `htap` must be provided directly after the `--dsn` argument.
To run an HTAP benchmark with 4 OLTP workers and 2 OLAP workers for 30 minutes, run the folowing:

    ./run_benchmark \
        --dsn postgresql://postgres@localhost/htap
        [--benchmark] htap \
        --oltp-workers 4 \
        --olap-workers 2 \
        --duration 1800

## Required Parameters

Parameter      | Description
-------------- | -----------------------------------------------
`dsn`          | The full DSN of the DB to connect to. DSN layout: <pre>postgresql://&lt;user&gt;@&lt;host&gt;:&lt;target-port&gt;/&lt;target-db&gt;</pre> The port is optional and the default is 5432. Example with port 5444 and use of EPAS: <pre>--dsn postgresql://enterprisedb@localhost:5444/example-database</pre>
`htap`         | Enables parsing of the command line arguments below, do not prefix with `--`.

## Optional Parameters

Parameter             | Description
--------------------- | -----------------------------------------------
`oltp-workers`        | The number of OLTP workers executing TPC-C transactions (i.e. simulated clients), default: 1
`olap-workers`        | The number of OLAP workers running modified TPC-H queries, default: 1.
`duration`            | The number of seconds the benchmark should run for, default: 60 seconds
`olap-timeout`        | Timeout for OLAP queries in seconds, default: 900
`dry-run`             | Only generate transactions and queries but don't send them to the DB. Can be useful for measuring script throughput.
`monitoring-interval` | Number of seconds to wait between updates of the monitoring display, default: 1
`stats-dsn`           | The DSN to use for collecting statistics into a database. Not defining it will disable statistics collection.

## Monitoring

During a benchmark run the HTAP benchmark presents you with the following monitoring screen.
This requires a VT100 compatible terminal emulator.

    Detected scale factor: 1                                 <- scale factor, detected by counting the number of warehouses
    Database statistics collection is disabled.              <- this will be shown if you didn't provide a `stats-dsn`
    OK  -> Total TX:         87 | Current rate:   58.0 tps   <- the current transaction rate (tansactions per second)
    ERR -> Total TX:          1 | Current rate:    0.0 tps   <- the current error rate (failed transactions per second)

    Stream   |    1      |    2      |                       <- one column per OLAP stream
    ----------------------------------
    Query  1 |           |           |                       <- The state of each query that was
    Query  2 |      0.43 |           |                          recently run or is running currently.
    Query  3 |           |      0.72 |                          Also shows when a query timed out or
    Query  4 |           |           |                          caused an error in the database.
    Query  5 |           |           |                          For finished queries the runtime is
    Query  6 |      0.07 |           |                          displayed.
    Query  7 |           |           |
    Query  8 |           |           |
    Query  9 |      0.63 |           |
    Query 10 |           |           |
    Query 11 |           |           |
    Query 12 |           |           |
    Query 13 |           |           |
    Query 14 |      0.25 |           |
    Query 15 |           |           |
    Query 16 |           |           |
    Query 17 |  Running  |           |
    Query 18 |           |  Running  |
    Query 19 |           |           |
    Query 20 |      0.45 |           |
    Query 21 |           |      0.74 |
    Query 22 |           |           |

    Elapsed: 2 seconds


# Testing

For testing, install the test requirements,

    /usr/bin/python3.6 -m pip install -r requirements-test.txt

and run `python -m pytest tests`. Some benchmark modules provide their own tests. To run, for example
the test for the HTAP benchmark, execute `python -m pytest benchmarks/htap/tests`.
