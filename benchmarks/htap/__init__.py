from benchmarks.htap.lib.controller import HTAPController


def add_parser(subparsers):
    parser = subparsers.add_parser('htap')
    parser.add_argument(
        '--oltp-workers', default=32, type=int, help=(
        'The number of OLTP workers executing TPC-C-like transactions (i.e. simulated clients), default: 32.'))

    parser.add_argument(
        '--olap-workers', default=1, type=int, help=(
        'The number of OLAP workers (streams) running TPC-H-like queries, default: 1.'))

    parser.add_argument(
        '--target-tps', default=None, type=int, help=(
        'The target TPS for the OLTP workload, default: unlimited.'))

    parser.add_argument(
        '--duration', default=60, type=int, help=(
        'How many seconds the benchmark should run for, default: 60.'))

    parser.add_argument(
        '--olap-timeout', default='5min', help=(
        'Timeout for OLAP queries, default: 5 minutes'))

    parser.add_argument(
        '--csv-interval', default=10, type=int, help=(
        'How often to report stats to the csv files in seconds, default: 10'))

    parser.add_argument(
        '--dry-run', action='store_true', help=(
        "Only generate transactions and analytical queries but don't send them to the database. "
        "Can be useful for measuring script throughput."))

    parser.add_argument(
        '--monitoring-interval', default=1, type=float, help=(
        'Number of seconds to wait between updates of the monitoring display, default: 1.0'))

    parser.add_argument(
        '--stats-dsn', help=('The DSN to use for collecting statistics into a database. '
        'Not defining it will disable statistics collection.'))

    parser.add_argument('--explain-analyze', action='store_true', default=False,
            help=('Whether to run EXPLAIN ANALYZE. Will save plans into the "plan" directory.'
        ))

    parser.add_argument('--use-server-side-cursors', default=False, action='store_true',
        required=False, help=('Use server-side cursors for executing the queries')
    )

    parser.add_argument('--dont-wait-until-enough-data', default=False, action='store_true',
        required=False, help=('Do NOT wait until there is enough data for OLAP queries to run with a constant dataset size')
    )
    
    parser.add_argument('--olap-dsns', nargs='+',
        required=False, help=('Use separate olap servers')
    )

    parser.add_argument('--output', choices=['csv', 'print'], default='print',
        nargs='+', help=('How the results output should look like. '
        'Multiple options possible, separated by space'
    ))

    parser.add_argument('--csv-file', default='results.csv', help=(
        'Where to save the summary csv file, if csv output is selected. '
        'The default is results.csv in the current directory.'
    ))

    parser.add_argument('--ignored-queries', required=False, nargs='+', default=[], help=(
        'Optional list of ignored queries for the OLAP workload.'
    ))

def run(args):
    controller = HTAPController(args)
    controller.run()
