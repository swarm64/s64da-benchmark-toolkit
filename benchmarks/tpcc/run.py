
import sys

from datetime import datetime

from benchmarks.tpcc.lib import workers


def add_args(subparsers):
    parser = subparsers.add_parser('tpcc')
    parser.add_argument(
        '--oltp-workers', required=True, type=int, help=(
        'How many workers to generate new orders.'))

    parser.add_argument(
        '--olap-workers', required=True, type=int, help=(
        'How many query streams to run in parallel'))

#    parser.add_argument(
#        '--olap-timestamp', type=datetime.fromisoformat, help=(
#        'Timestamp to use for OLAP query when there is no OLTP'))

    parser.add_argument(
        '--start-date', required=True, type=datetime.fromisoformat, help=(
        'First date of new orders, should be one day after ingestion date.'))

    parser.add_argument(
        '--end-date', required=True, type=datetime.fromisoformat, help=(
        'Date until to run. Process will abort once this date is hit first.'))

    parser.add_argument(
        '--orders-per-day', required=True, type=int, help=(
        'How many orders per day to generate.'))

    parser.add_argument(
        '--dummy-db', action='store_true', help=(
        'Do not actually write to the DB. Useful to measure script capacity.'))

    parser.add_argument(
        '--olap-timeout', default=900, type=int, help=(
        'Query timeout for OLAP queries in seconds. Default: 900'))

def run(args):
    workers.run_impl(args)
