import time

from datetime import datetime, timedelta
from math import trunc
from uuid import uuid4

from psycopg2.extras import execute_values, register_uuid

from .queries import Queries

register_uuid()

MONITORING_INTERVAL = 1


class Stats:
    """
    A stats collector for TPC-C and TPC-H stats.
    The way TPC-C and TPC-H report their stats are quite different.

    TPC-C
    -----
    Since TPC-C has a very high througput (thousands per second) not
    every finished transaction is reported by the `transactions` module.
    Instead, the number of successfull, failed, and new-order transactions
    is recorded and put into the monitoring queue once per monitoring interval.

    TPC-H
    -----
    For TPC-H the status of each individual query (Running, Finished, Error,
    Timeout) is put into the monitoring queue, regardless of monitoring interval.
    """
    def __init__(self, num_oltp_slots, num_olap_slots):
        self.data = {
            'tpcc': [],
            'tpcc_ok_previous': 0,
            'tpcc_err_previous': 0,
            'tpch': []
        }
        self.data['tpcc'] = [{'ok_count': 0, 'err_count': 0, 'new_order_count': 0}
                             for _ in range(num_oltp_slots)]
        self.data['tpch'] = [{
                'queries': {},
                'finished_count': 0,
                'error_count': 0,
                'timeout_count': 0
        } for _ in range(num_olap_slots)]
        self.latest_delivery_date = None
        self.tpch_buffer = []
        self.uuid = uuid4()

    def _update_tpcc_stats(self, stats):
        worker_id = stats['worker_id']
        self.data['tpcc'][worker_id]['ok_count'] = stats['ok_count']
        self.data['tpcc'][worker_id]['err_count'] = stats['err_count']
        self.data['tpcc'][worker_id]['new_order_count'] = stats['new_order_count']

        if 'latest_delivery_date' in stats:
            latest_delivery_date = stats['latest_delivery_date']
            if latest_delivery_date and self.latest_delivery_date:
                self.latest_delivery_date = max(self.latest_delivery_date, latest_delivery_date)
            elif latest_delivery_date is not None:
                self.latest_delivery_date = latest_delivery_date

    def _update_tpch_stats(self, stats, time_now):
        stream_id = stats['stream']
        query_id = stats['query']

        if stats['status'] == 'Running':
            self.data['tpch'][stream_id]['queries'][query_id] = 'Running'
        elif stats['status'] == 'Finished':
            runtime = stats['runtime']
            self.data['tpch'][stream_id]['queries'][query_id] = runtime
            self.data['tpch'][stream_id]['finished_count'] += 1
            self.tpch_buffer.append((self.uuid, time_now, stream_id, query_id, runtime))
        elif stats['status'] == 'Error':
            self.data['tpch'][stream_id]['queries'][query_id] = 'Error'
            self.data['tpch'][stream_id]['error_count'] += 1
        elif stats['status'] == 'Canceled':
            self.data['tpch'][stream_id]['queries'][query_id] = 'Timeout'
            self.data['tpch'][stream_id]['timeout_count'] += 1

    def _update_stats(self, src, item, time_now):
        if src == 'tpcc':
            self._update_tpcc_stats(item)
        elif src == 'tpch':
            self._update_tpch_stats(item, time_now)

    def set_latest_delivery_date(self, date):
        self.latest_delivery_date = date

    def get_latest_delivery_date(self):
        return self.latest_delivery_date

    def update(self, reporting_queue, time_now):
        while not reporting_queue.empty():
            src, item = reporting_queue.get_nowait()
            self._update_stats(src, item, time_now)

    def consume_stats(self):
        tpcc_ok_sum, tpcc_err_sum, _ = self.tpcc_totals()
        tpcc_ok_previous = self.data['tpcc_ok_previous']
        tpcc_err_previous = self.data['tpcc_err_previous']
        self.data['tpcc_ok_previous'] = tpcc_ok_sum
        self.data['tpcc_err_previous'] = tpcc_err_sum

        return (tpcc_ok_sum, tpcc_ok_sum - tpcc_ok_previous,
                tpcc_err_sum, tpcc_err_sum - tpcc_err_previous)

    def tpcc_stats_for_stream_id(self, stream_id):
        return self.data['tpcc'][stream_id]

    def tpch_stats_for_stream_id(self, stream_id):
        return self.data['tpch'][stream_id]

    def tpcc_totals(self):
        ok_total = 0
        err_total = 0
        new_order_total = 0

        for tpcc in self.data['tpcc']:
            ok_total += tpcc['ok_count']
            err_total += tpcc['err_count']
            new_order_total += tpcc['new_order_count']

        return ok_total, err_total, new_order_total

    def tpch_totals(self):
        tpch_data = self.data['tpch']
        return (
                sum(slot['finished_count'] for slot in tpch_data),
                sum(slot['error_count'] for slot in tpch_data),
                sum(slot['timeout_count'] for slot in tpch_data)
        )


class Monitor:
    def __init__(self, stats, num_oltp_workers, num_olap_workers, scale_factor, interval):
        self.stats = stats
        self.num_oltp_workers = num_oltp_workers
        self.num_olap_workers = num_olap_workers
        self.scale_factor = scale_factor
        self.output = None
        self.current_line = 0
        self.interval = interval
        self.total_lines = (
                2  # OLTP
                + 1  # empty
                + 2  # OLAP header
                + len(Queries.query_ids())  # OLAP queries
                + 2  # footer
        )

    def _update_stats(self, reporting_queue):
        while not reporting_queue.empty():
            src, item = reporting_queue.get_nowait()
            self.add_stats(src, item)

    def _add_display_line(self, line):
        if self.current_line > 0 and self.current_line % self.total_lines == 0:
            # Go up `self.total_lines` lines
            print(self.total_lines * '\033[F', end='')

        # Delete current line
        print('\033[2K', end='')
        print(line)
        self.current_line += 1

    def display_summary(self, elapsed):
        elapsed_seconds = elapsed.total_seconds()
        tpcc_ok_sum, tpcc_err_sum, tpcc_new_order_sum = self.stats.tpcc_totals()
        tps = tpcc_ok_sum / elapsed_seconds
        eps = tpcc_err_sum / elapsed_seconds
        tpmc = trunc((tpcc_new_order_sum / elapsed_seconds) * 60)
        num_queries, num_errors, num_timeouts = self.stats.tpch_totals()
        throughput = num_queries * 3600 / elapsed_seconds
        print()
        summary = 'Summary'
        print(f'{summary}\n' + len(summary) * '-')
        print(f'Scale Factor: {self.scale_factor}')
        print(f'Streams: {self.num_oltp_workers} OLTP, {self.num_olap_workers} OLAP')
        print(f'Total time: {elapsed_seconds:.2f} seconds')
        print(f'TPC-C Transactions per second (TPS): {tps:.2f}')
        print(f'TPC-C Errors per second: {eps:.2f}')
        print(f'TPC-C New-Order transactions per minute (tpmC): {tpmc:.0f}')
        print(f'TPC-H Throughput (queries per hour): {throughput:.1f}')
        print(f'TPC-H Errors {num_errors:}, Timeouts: {num_timeouts:}')

    def get_elapsed_row(self, elapsed_seconds):
        unit = 'second' if elapsed_seconds < 2 else 'seconds'
        return f'Elapsed: {elapsed_seconds:.0f} {unit}'

    def get_tpcc_row(self, prefix, total, rate):
        return f'{prefix} -> Total TX: {total:10} | Current rate: {rate:6} tps'

    def get_olap_header(self):
        olap_header = f'{"Stream":<8} |'
        olap_header += ''.join([f'{x:^10d} |' for x in range(1, self.num_olap_workers + 1)])
        return olap_header

    def get_olap_row(self, query_id):
        row = f'Query {query_id:2d} |'
        for stream_id in range(self.num_olap_workers):
            query_info = self.stats.tpch_stats_for_stream_id(stream_id).get('queries').get(query_id)
            if query_info and type(query_info) == float:
                row += f' {query_info:9.2f} |'
            elif query_info:
                row += f' {query_info : ^9} |'
            else:
                row += f' {" ":9} |'
        return row

    def _write_to_db(self, conn, time_now, tpcc_ok_rate, tpcc_err_rate):
        conn.cursor.execute(
            'INSERT INTO oltp_stats VALUES(%s, %s, %s, %s)',
            (self.stats.uuid, time_now, tpcc_ok_rate, tpcc_err_rate))

        if self.stats.tpch_buffer:
            sql = 'INSERT INTO olap_stats VALUES %s'
            execute_values(conn.cursor, sql, self.stats.tpch_buffer)
            self.stats.tpch_buffer = []

    def update_display(self, time_elapsed, time_now, stats_conn):
        assert MONITORING_INTERVAL == 1, 'only 1 second intervals are supported'
        tpcc_ok_sum, tpcc_ok_in_interval, tpcc_err_sum, tpcc_err_in_interval = \
                self.stats.consume_stats()

        tpcc_ok_rate = tpcc_ok_in_interval / self.interval
        tpcc_err_rate = tpcc_err_in_interval / self.interval

        self._add_display_line(self.get_tpcc_row('OK ', tpcc_ok_sum, tpcc_ok_rate))
        self._add_display_line(self.get_tpcc_row('ERR', tpcc_err_sum, tpcc_err_rate))
        self._add_display_line('')
        olap_header = self.get_olap_header()
        self._add_display_line(olap_header)
        self._add_display_line('-' * len(olap_header))

        for query_id in Queries.query_ids():
            self._add_display_line(self.get_olap_row(query_id))

        if stats_conn is not None:
            self._write_to_db(stats_conn, time_now, tpcc_ok_rate, tpcc_err_rate)

        self._add_display_line('')
        self._add_display_line(self.get_elapsed_row(time_elapsed))
