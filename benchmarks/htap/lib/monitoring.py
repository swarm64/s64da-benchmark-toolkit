from dateutil.relativedelta import relativedelta
from math import trunc
from tabulate import tabulate

from benchmarks.htap.lib.analytical import QUERY_IDS
from benchmarks.htap.lib.helpers import WAREHOUSES_SF_RATIO
from benchmarks.htap.lib.stats import QUERY_TYPES, HISTORY_LENGTH

class Monitor:
    def __init__(self, stats, num_oltp_workers, num_olap_workers, num_warehouses, min_timestamp):
        self.stats = stats
        self.num_oltp_workers = num_oltp_workers
        self.num_olap_workers = num_olap_workers
        self.num_warehouses = num_warehouses
        self.output = None
        self.current_line = 0
        self.total_lines = 0
        self.min_timestamp = min_timestamp.date()
        self.lines = []

    def _add_display_line(self, line):
        self.lines.append(line)

    def _print(self):
        print(self.total_lines * '\033[F', end='')
        for line in self.lines:
            print('\033[2K', end='')
            print(line)
        self.total_lines = len(self.lines)
        self.lines = []

    def display_summary(self, elapsed):
        print()
        summary = 'Summary'
        print(f'{summary}\n' + len(summary) * '-')
        print(f'Scale factor: {self.num_warehouses // WAREHOUSES_SF_RATIO }')
        print(f'Workers: {self.num_oltp_workers} OLTP, {self.num_olap_workers} OLAP')

        elapsed_seconds = max(1, elapsed.total_seconds())
        print(f'Total time: {elapsed_seconds:.2f} seconds')
        # TODO output time after burn-in step

        print('')
        print(f'OLTP')
        rows = []
        _, tps, latency = self.stats.oltp_total()
        rows.append(['All', f'{tps[2]:.2f}', f'{latency[2]:.2f}'])
        for query_type in QUERY_TYPES:
            _, tps, latency = self.stats.oltp_total(query_type)
            rows.append([query_type, f'{tps[2]:.2f}', f'{latency[2]:.2f}'])
        print(tabulate(rows, headers=['', 'Average transactions per second',  'Average latency (ms)']))

        print('')
        print(f'OLAP')
        rows = []
        avg_stream_runtime, n_streams = self.stats.olap_stream_totals()
        rows.append(['Average stream runtime (ms)', avg_stream_runtime])
        rows.append(['Completed stream iterations', n_streams])
        num_ok, num_errors, num_timeouts = self.stats.olap_totals()
        rows.append(['Succesful queries', num_ok])
        rows.append(['Queries with errors', num_errors])
        rows.append(['Query timeouts', num_timeouts])
        print(tabulate(rows))
        # TODO output average query runtime per query type

    def get_elapsed_row(self, elapsed_seconds):
        unit = 'second' if elapsed_seconds < 2 else 'seconds'
        return f'Elapsed: {elapsed_seconds:.0f} {unit}'

    def get_oltp_row(self, query_type = None):
        counters, tps, latency = self.stats.oltp_total(query_type)
        issued, ok , err = counters
        tps     = '{:5} | {:5} | {:5} | {:5}'.format(*tps)
        latency = '{:5} | {:5} | {:5} | {:5}'.format(*latency)
        name = 'All types' if query_type == None else query_type
        return f'| {name:^12} | {issued:8} | {ok:10} | {err:6} | {tps} | {latency} |'

    def get_olap_header(self):
        olap_header = f'{"Stream":<8} |'
        olap_header += ''.join([f'{x:^10d} |' for x in range(1, self.num_olap_workers + 1)])
        olap_header += f' {"#rows planned":13} | {"#rows processed":14} |'
        return olap_header

    def get_olap_row(self, query_id):
        row = f'Query {query_id:2d} |'
        max_planned = 0
        max_processed = 0
        for stream_id in range(self.num_olap_workers):
            stats = self.stats.olap_stats_for_stream_id(stream_id).get('queries').get(query_id)
            if stats and stats['runtime'] > 0:
                # output last result
                max_planned = max(max_planned, int(stats['planned_rows']/1000))
                max_processed = max(max_processed, int(stats['processed_rows']/1000))
                row += '{:7.2f} {:3}|'.format(stats['runtime'], stats['status'][:3].upper())
            elif stats:
                # output a state
                row += ' {:^10}|'.format(stats['status'])
            else:
                row += f' {" ":9} |'

        row += f' {max_planned:12}K | {max_processed:13}K |'
        return row

    def get_olap_sum(self):
        row = f'Total    |'
        for stream_id in range(self.num_olap_workers):
            stats = self.stats.olap_stats_for_stream_id(stream_id)
            stream_sum = sum(stats['queries'][query_id]['runtime'] for query_id in QUERY_IDS)
            row += f' {stream_sum:9.2f} |'
        return row

    def get_columnstore_row(self, row):
        return f'| {row[0]:^12} | {row[1]:7.2f}GB | {row[2]:7.2f}GB | {row[3]:6.2f}x | {row[4]:4.2f}% |'

    def update_display(self, time_elapsed, time_now, stats_conn, latest_timestamp):
        latest_time = latest_timestamp.date()
        date_range = relativedelta(latest_time, self.min_timestamp)

        self.current_line = 0
        data_warning = "(not enough for consistent OLAP queries)" if date_range.years < 7 else ""

        self._add_display_line(f'Data range: {self.min_timestamp} - {latest_time} = {date_range.years} years, {date_range.months} months and {date_range.days} days {data_warning}')
        self._add_display_line(f'DB size: {self.stats.db_size()}')
        if self.stats.columnstore_stats():
            self._add_display_line('')
            self._add_display_line('Table size and S64 DA columnstore status')
            self._add_display_line('-----------------------------------------------------------')
            self._add_display_line('|    table     | heap size |  colstore |  ratio  | cached |')
            self._add_display_line('|---------------------------------------------------------|')
            for row in self.stats.columnstore_stats():
                self._add_display_line(self.get_columnstore_row(row))
            self._add_display_line('-----------------------------------------------------------')
        self._add_display_line('')
        self._add_display_line('OLTP workload status')
        self._add_display_line('-----------------------------------------------------------------------------------------------------------------')
        self._add_display_line('|     TYPE     |  ISSUED  |  COMPLETED | ERRORS |         TPS (last {}s)        |   LATENCY (last {}s, in ms)   |'.format(HISTORY_LENGTH, HISTORY_LENGTH))
        self._add_display_line('|              |          |            |        |  CURR |  MIN  |  AVG  |  MAX  |  CURR |  MIN  |  AVG  |  MAX  |')
        self._add_display_line('|---------------------------------------------------------------------------------------------------------------|')
        for query_type in QUERY_TYPES:
            self._add_display_line(self.get_oltp_row(query_type))
        self._add_display_line('|---------------------------------------------------------------------------------------------------------------|')
        self._add_display_line(self.get_oltp_row())
        self._add_display_line('-----------------------------------------------------------------------------------------------------------------')

        if self.num_olap_workers > 0:
            self._add_display_line('')
            self._add_display_line('OLAP workload status')
            olap_header = self.get_olap_header()
            self._add_display_line('-' * len(olap_header))
            self._add_display_line(olap_header)
            self._add_display_line('-' * len(olap_header))

            for query_id in QUERY_IDS:
                self._add_display_line(self.get_olap_row(query_id))
            self._add_display_line('-' * len(olap_header))
            self._add_display_line(self.get_olap_sum())
            self._add_display_line('-' * len(olap_header))

        self._add_display_line('')
        self._add_display_line(self.get_elapsed_row(time_elapsed))
        self._print()
