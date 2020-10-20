
import time

from datetime import datetime
from uuid import uuid4

from psycopg2.extras import execute_values, register_uuid
from reprint import output

from .queries import QUERIES

register_uuid()

QUERIES_LIST = sorted(QUERIES.keys())
QUERIES_OUTPUT_MAP = {QUERIES_LIST[idx]: idx for idx in range(len(QUERIES_LIST))}


class Monitor:
    def __init__(self, args):
        self.uuid = uuid4()
        self.display_idx = 0
        self.olap_workers = args.olap_workers
        self.oltp_workers = args.oltp_workers
        self.end_date = args.end_date
        self.stats = {
            'tpcc': [],
            'tpcc_ok_previous': 0,
            'tpcc_err_previous': 0,
            'tpch': []
        }
        self.stats['tpcc'] = [
            {'ok_count': 0, 'err_count': 0} for _ in range(args.oltp_workers)]

        self.stats['tpch'] = [{} for _ in range(args.olap_workers)]
        self.tpch_buffer = []
        self.current_timestamp = datetime.now()

    def past_end_date(self, max_timestamp):
        return max_timestamp and max_timestamp >= self.end_date

    def add_stats(self, src, item):
        if src == 'tpcc':
            self._add_tpcc_stats(item)
        elif src == 'tpch':
            self._add_tpch_stats(item)

    def _add_tpcc_stats(self, stats):
        worker_id = stats['worker_id']
        self.stats['tpcc'][worker_id]['ok_count'] = stats['ok_count']
        self.stats['tpcc'][worker_id]['err_count'] = stats['err_count']

    def _add_tpch_stats(self, stats):
        stream_id = stats['stream']
        query_id = stats['query']

        if stats['status'] == 'Running':
            self.stats['tpch'][stream_id][query_id] = 'Running'
        elif stats['status'] == 'Finished':
            runtime = stats['runtime']
            self.stats['tpch'][stream_id][query_id] = runtime
            self.tpch_buffer.append((self.uuid, self.current_timestamp, stream_id, query_id, runtime))

    def _update_stats(self, reporting_queue):
        while not reporting_queue.empty():
            src, item = reporting_queue.get_nowait()
            self.add_stats(src, item)

    def _add_display_line(self, output_list, line):
        output_list[self.display_idx] = line
        self.display_idx += 1

    def _update_tpcc_stats(self):
        tpcc_ok_sum = 0
        tpcc_err_sum = 0
        tpcc_ok_previous = self.stats['tpcc_ok_previous']
        tpcc_err_previous = self.stats['tpcc_err_previous']
        for tpcc in self.stats['tpcc']:
            tpcc_ok_sum += tpcc['ok_count']
            tpcc_err_sum += tpcc['err_count']
        self.stats['tpcc_ok_previous'] = tpcc_ok_sum
        self.stats['tpcc_err_previous'] = tpcc_err_sum

        return (tpcc_ok_sum, tpcc_ok_sum - tpcc_ok_previous,
                tpcc_err_sum, tpcc_err_sum - tpcc_err_previous)

    def _write_to_db(self, conn, tpcc_ok_rate, tpcc_err_rate):
        conn.cursor.execute(
            'INSERT INTO oltp_stats VALUES(%s, %s, %s, %s)',
            (self.uuid, self.current_timestamp, tpcc_ok_rate, tpcc_err_rate))

        if self.tpch_buffer:
            sql = 'INSERT INTO olap_stats VALUES %s'
            execute_values(conn.cursor, sql, self.tpch_buffer)
            self.tpch_buffer = []

    def display(self, reporting_queue, timestamps, dbconn):
        def get_timestamp_row(min_ts, max_ts):
            return f'Min time: {min_ts} | Max time: {max_ts}'

        def get_tpcc_row(prefix, total, rate):
            return f'{prefix} -> Total TX: {total:10} | Current rate: {rate:6}tps'

        def get_olap_header():
            olap_header = f'{"Stream":<8} |'
            olap_header += ''.join([f'{x:^10d} |' for x in range(1, self.olap_workers + 1)])
            return olap_header

        def get_olap_row(query_id):
            row = f'Query {query_id:2d} |'
            for stream_id in range(self.olap_workers):
                query_info = self.stats['tpch'][stream_id].get(query_id)
                if query_info and type(query_info) == float:
                    row += f' {query_info:9.2f} |'
                elif query_info:
                    row += f' {query_info : ^9} |'
                else:
                    row += f' {" ":9} |'
            return row

        def convert_ts(ts):
            return datetime.fromtimestamp(ts)

        with output(output_type="list", initial_len=20, interval=0) as output_list:
            while True:
                time.sleep(1.0)
                self.display_idx = 0
                self.current_timestamp = datetime.now()
                self._update_stats(reporting_queue)
                timestamps_filtered = [convert_ts(timestamps[idx]) for idx in range(self.oltp_workers)]
                min_timestamp = min(timestamps_filtered)
                max_timestamp = max(timestamps_filtered)

                tpcc_ok_sum, tpcc_ok_rate, tpcc_err_sum, tpcc_err_rate = self._update_tpcc_stats()

                self._add_display_line(output_list, str(self.uuid))
                self._add_display_line(output_list, get_timestamp_row(min_timestamp, max_timestamp))
                self._add_display_line(output_list, get_tpcc_row('OK ', tpcc_ok_sum, tpcc_ok_rate))
                self._add_display_line(output_list, get_tpcc_row('ERR', tpcc_err_sum, tpcc_err_rate))
                self._add_display_line(output_list, ' ')
                self._add_display_line(output_list, get_olap_header())

                for query_id in QUERIES_LIST:
                    self._add_display_line(output_list, get_olap_row(query_id))

                self._write_to_db(dbconn, tpcc_ok_rate, tpcc_err_rate)

                if self.oltp_workers > 0 and self.past_end_date(max_timestamp):
                    break
