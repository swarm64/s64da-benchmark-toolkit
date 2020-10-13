
import sys
import time

from datetime import datetime
from uuid import uuid4

from psycopg2.extras import execute_values, register_uuid
from reprint import output

from .queries import QUERIES
from s64da_benchmark_toolkit.dbconn import DBConn

register_uuid()

QUERIES_LIST = sorted(QUERIES.keys())
QUERIES_OUTPUT_MAP = {QUERIES_LIST[idx]: idx for idx in range(len(QUERIES_LIST))}


class TxView:
    def __init__(self):
        self.counter = 0
        self.metrics = {
            'ok': {
                'min': sys.maxsize,
                'max': 0,
                'avg': 0,
                'current': 0,
                'total': 0
            },
            'err': {
                'min': sys.maxsize,
                'max': 0,
                'avg': 0,
                'current': 0,
                'total': 0
            },
            'total': {
                'min': sys.maxsize,
                'max': 0,
                'avg': 0,
                'current': 0,
                'total': 0
            }
        }

    def add_tx_info(self, ok, err):
        self.metrics['ok']['current'] = ok
        self.metrics['ok']['total'] += ok

        self.metrics['err']['current'] = err
        self.metrics['err']['total'] += err

        self.metrics['total']['current'] = (ok + err)
        self.metrics['total']['total'] += (ok + err)

        self.counter += 1

        for key in ('ok', 'err', 'total'):
            entry = self.metrics[key]
            entry['avg'] = entry['total'] / self.counter
            entry['min'] = min(entry['min'], entry['current'])
            entry['max'] = max(entry['max'], entry['current'])

    @property
    def output(self):
        output = []
        for key in ('ok', 'err', 'total'):
            entry = self.metrics[key]
            s  = f"Tx {key:>6} - "
            s += f"Now: {entry['current']:>5.0f} tps  "
            s += f"AVG: {entry['avg']:>5.0f} tps  "
            s += f"MIN: {entry['min']:>5.0f} tps  "
            s += f"MAX: {entry['max']:>5.0f} tps  "
            s += f"TOTAL: {entry['total']: >10,.0f}"
            output.append(s)
        return output

class QueryRuntimeView:
    class Buffer:
        def __init__(self, capacity):
            self.counter = 0
            self._data = [None] * capacity

        def push(self, datum):
            self.counter += 1
            self._data.insert(0, datum)
            self._data.pop()

        @property
        def all(self):
            return (self.counter, self._data)

    def __init__(self, query_num):
        self.query_num = query_num
        self.status = '-'
        self.runtimes = QueryRuntimeView.Buffer(3)

    def add_runtime(self, runtime):
        self.runtimes.push(round(runtime, 3))

    def set_status(self, status):
        self.status = status

    def __str__(self):
        counter, runtimes = self.runtimes.all
        runtimes = ' | '.join([f'{x:>10.3f}' for x in runtimes if x])
        return f'Q{self.query_num:<3}: {counter:>3}x - {self.status:<8} - {runtimes}'


class Output:
    def __init__(self, dsn):
        self.uuid = uuid4()
        self.dsn = dsn
        self.tx_view = TxView()
        self.query_runtimes = []
        for query in QUERIES_LIST:
            self.query_runtimes.append(QueryRuntimeView(query))

    def display(self, shared, end_timestamp, oltp_workers):
        with output(output_type="list", initial_len=20, interval=0) as output_list:
            output_list[0] = self.uuid
            with DBConn(self.dsn) as dbconn:
                for idx in range(1,20):
                    output_list[idx] = ' '

                while not shared.stop.is_set():
                    counter_ok_tmp = 0
                    counter_err_tmp = 0
                    time.sleep(1)
                    with shared.lock:
                        counter_ok_tmp = shared.counter_ok.value
                        counter_err_tmp = shared.counter_err.value
                        shared.counter_ok.value = 0
                        shared.counter_err.value = 0

                    current_timestamp = datetime.now()
                    timestamp = datetime.fromtimestamp(shared.order_timestamp.value)
                    self.tx_view.add_tx_info(counter_ok_tmp, counter_err_tmp)

                    dbconn.cursor.execute(
                        'INSERT INTO oltp_stats VALUES(%s, %s, %s, %s)',
                        (self.uuid, current_timestamp, counter_ok_tmp, counter_err_tmp))

                    db_buffer = []
                    while not shared.queries_queue.empty():
                        worker_id, query, runtime = shared.queries_queue.get_nowait()
                        output_idx = QUERIES_OUTPUT_MAP.get(query, None)
                        if output_idx is not None and isinstance(runtime, float):
                            self.query_runtimes[output_idx].add_runtime(runtime)
                            self.query_runtimes[output_idx].set_status('done')
                            db_buffer.append((self.uuid, current_timestamp, worker_id, query, runtime))

                    if db_buffer:
                        sql = 'INSERT INTO olap_stats VALUES %s'
                        execute_values(dbconn.cursor, sql, db_buffer)

                    output_list[1] = f'Current timestamp: {timestamp}'
                    output_list[2], output_list[3], output_list[4] = self.tx_view.output
                    for idx, entry in enumerate(self.query_runtimes):
                        output_list[idx + 6] = str(entry)

                    if oltp_workers > 0 and timestamp > end_timestamp:
                        return True
