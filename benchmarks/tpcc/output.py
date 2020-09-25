
import time

from datetime import datetime

from reprint import output

from benchmarks.tpcc.queries import QUERIES


QUERIES_LIST = sorted(QUERIES.keys())
QUERIES_OUTPUT_MAP = {QUERIES_LIST[idx]: idx for idx in range(len(QUERIES_LIST))}


class TxView:
    def __init__(self):
        self.ok_current = 0
        self.ok_sum = 0
        self.err_current = 0
        self.err_sum = 0
        self.total_current = 0
        self.total_sum = 0
        self.counter = 0

    def add_tx_info(self, ok, err):
        self.ok_current = ok
        self.ok_sum += ok
        self.err_current = err
        self.err_sum += err
        self.total_current = ok + err
        self.total_sum += (ok + err)
        self.counter += 1

    @property
    def output(self):
        return (
            f'Tx OK    : {self.ok_current:>10.0f} AVG: {(self.ok_sum / self.counter):>10.0f} Total: {self.ok_sum:,}',
            f'Tx ERR   : {self.err_current:>10.0f} AVG: {(self.err_sum / self.counter):>10.0f} Total: {self.err_sum:,}',
            f'Tx Total : {self.total_current:>10.0f} AVG: {(self.total_sum / self.counter):>10.0f} Total: {self.total_sum:,}'
        )

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
    def __init__(self):
        self.tx_view = TxView()
        self.query_runtimes = []
        for query in QUERIES_LIST:
            self.query_runtimes.append(QueryRuntimeView(query))

    def display(self, lock, counter_ok, counter_err, order_timestamp, query_queue):
        with output(output_type="list", initial_len=18, interval=0) as output_list:
            for idx in range(15):
                output_list[idx] = ' '

            while True:
                time.sleep(1)
                with lock:
                    self.tx_view.add_tx_info(counter_ok.value, counter_err.value)
                    timestamp = datetime.fromtimestamp(order_timestamp.value)
                    counter_ok.value = 0
                    counter_err.value = 0

                while not query_queue.empty():
                    query, runtime = query_queue.get_nowait()
                    output_idx = QUERIES_OUTPUT_MAP.get(query, None)
                    if output_idx is not None and isinstance(runtime, float):
                        self.query_runtimes[output_idx].add_runtime(runtime)
                        self.query_runtimes[output_idx].set_status('done')
                    elif output_idx:
                        self.query_runtimes[output_idx].set_status(runtime)

                output_list[0] = f'Current timestamp: {timestamp}'
                output_list[1], output_list[2], output_list[3] = self.tx_view.output
                for idx, entry in enumerate(self.query_runtimes):
                    output_list[idx + 5] = str(entry)
