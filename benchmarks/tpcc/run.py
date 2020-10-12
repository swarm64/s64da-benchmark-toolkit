
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

    parser.add_argument(
        '--olap-timestamp', type=datetime.fromisoformat, help=(
        'Timestamp to use for OLAP query when there is no OLTP'))

    parser.add_argument(
        '--start-date', required=True, type=datetime.fromisoformat, help=(
        'First date of new orders, should be one day after ingestion date.'))

    parser.add_argument(
        '--end-date', required=True, type=datetime.fromisoformat, help=(
        'Date until to run. Process will abort once this date is hit first.'))

    parser.add_argument(
        '--orders-per-day', required=True, type=int, help=(
        'How many orders per day to generate.'))

def run(args):
    def on_error(what):
        print(f'Error called: {what}')
        sys.exit(1)

    try:
        workers.run_all(args, on_error)
    except KeyboardInterrupt:
        pass
