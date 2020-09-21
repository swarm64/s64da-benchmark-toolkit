# Quick start

## Data Ingest and Database Preparation

To ingest data for a TPC-H benchmark with scale factor 10 using this toolkit,
run the following command:

    ./prepare_benchmark \
        --dsn postgresql://postgres@localhost:5432/swarm64_benchmark \
        tpch \
        --scale-factor 10 \
        --schema s64da_performance

Adapt the values for the parameters `--benchmark` and `--scale-factor` for other
benchmarks and scale factors accordingly. If the system you want to benchmark
runs on a different host, you need to adapt the value for `--dsn` as well.

## Benchmark Running

To run a bechmark that has been prepared with the `prepare_benchmark` script,
execute the following command:

    ./run_benchmark \
        --dsn postgresql://postgres@localhost:5432/swarm64_benchmark \
        tpch

Adapt the value for the `--benchmark` parameter if you ingested data for a
different benchmark in the step above.

Please refer to `README.md` for a detailed description of all the available
parameters and options.
