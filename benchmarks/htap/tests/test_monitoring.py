import pytest

from datetime import datetime

from benchmarks.htap.lib.monitoring import Stats


class MockQueue:
    def __init__(self, data=[]):
        self.data = data

    def empty(self):
        return len(self.data) == 0

    def get(self):
        return self.data.pop(0)

dsn = "postgresql://postgres@127.0.0.1/htap"

def fill_waiting_queries(stats):
    for query in range(22):
        if not (query+1) in stats['queries']:
            stats['queries'][query+1] = {'runtime': 0, 'status': 'Waiting'}
    return stats

def test_storage():
    fixture = Stats(dsn, num_oltp_slots=4, num_olap_slots=0, tpcc_csv_interval=1)

    # yapf: disable
    data = [
            # this will be not be kept
            ('tpcc', [{ 'timestamp': 0, 'query': 1, 'runtime': 0.01 }]),
            ('tpcc', [{ 'timestamp': 0, 'query': 'ok', 'runtime': 0.01 }]),
            # from here on we have a 10s window
            ('tpcc', [{ 'timestamp': 1.1, 'query': 1, 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 1.1, 'query': 'ok', 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 2, 'query': 2, 'runtime': 0.02 }]),
            ('tpcc', [{ 'timestamp': 2, 'query': 'ok', 'runtime': 0.02 }]),
            ('tpcc', [{ 'timestamp': 2, 'query': 3, 'runtime': 0.03 }]),
            ('tpcc', [{ 'timestamp': 2, 'query': 'ok', 'runtime': 0.03 }]),
            ('tpcc', [{ 'timestamp': 2, 'query': 4, 'runtime': 0.04 }]),
            ('tpcc', [{ 'timestamp': 2, 'query': 'error', 'runtime': 0.04 }]),
            ('tpcc', [{ 'timestamp': 2.1, 'query': 1, 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 2.1, 'query': 'ok', 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 3.1, 'query': 1, 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 3.1, 'query': 'ok', 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 3.2, 'query': 1, 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 3.2, 'query': 'ok', 'runtime': 0.1 }]),
            ('tpcc', [{ 'timestamp': 11, 'query': 1, 'runtime': 1 }]),
            ('tpcc', [{ 'timestamp': 11, 'query': 'ok', 'runtime': 1 }]),
                ]
    # yapf: enable
    queue = MockQueue(data.copy())
    fixture.process_queue(queue)
    fixture.cleanup_tpcc_stats(11.1)

    # test:
    # - all samples up to 10s are kept
    # - tps needs at least 3 seconds and all stats are properly computed
    # - latencies are in ms and all stats are properly computed
    assert fixture.tpcc_tps(2) == (0, 0, 0, 0)
    assert fixture.tpcc_tps(3) == (0, 0, 0, 0)
    assert fixture.tpcc_tps(4) == (0, 0, 0, 0)
    # 1 surviving sample with 1tps and 1 with 2 tps, avg floored is therefore 1
    assert fixture.tpcc_tps(1) == (2, 0, 0, 2)
    # 3 tps @ 11s, 2tps @ 12
    assert fixture.tpcc_tps('ok') == (2, 0, 0, 3)

    assert fixture.tpcc_latency(2) == (20, 20, 20, 20)
    assert fixture.tpcc_latency(3) == (30, 30, 30, 30)
    assert fixture.tpcc_latency(4) == (40, 40, 40, 40)
    assert fixture.tpcc_latency(1) == (1000, 100, int((100+100+100+100+1000)/5), 1000)
    assert fixture.tpcc_latency('ok') == (1000, 20, int((100+20+30+100+100+100+1000)/7), 1000)
    
    assert fixture.tpcc_total('ok') == 8
    assert fixture.tpcc_total(1) == 6
    assert fixture.tpcc_total('error') == 1

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'TIMEOUT', 'runtime': 10.0}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.0}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'TIMEOUT', 'runtime': 13.5}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'ERROR', 'runtime': 0.1}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'OK', 'runtime': 7.0}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'ERROR', 'runtime': 0.2}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 2.3}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=4, tpcc_csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    # Newer values extend older values (except for query runtimes)
    # yapf: disable
    expected = {
            'queries': {1: {'query': 1, 'runtime': 2.3, 'status': 'OK', 'stream': 0}, 
                        7: {'query': 7, 'runtime': 7.0, 'status': 'OK', 'stream': 0},
                       },
            'ok_count': 3, 'error_count': 0, 'timeout_count': 1
    }
    assert fixture.tpch_stats_for_stream_id(0) == fill_waiting_queries(expected)
    expected = {
            'queries': {3: {'query': 3, 'runtime': 0.1, 'status': 'ERROR', 'stream': 1}},
            'ok_count': 0, 'error_count': 1, 'timeout_count': 0
    }
    assert fixture.tpch_stats_for_stream_id(1) == fill_waiting_queries(expected)
    expected = {
            'queries': {9: {'query': 9, 'runtime': 13.5, 'status': 'TIMEOUT', 'stream': 2}},
            'ok_count': 0, 'error_count': 0, 'timeout_count': 1
    }
    assert fixture.tpch_stats_for_stream_id(2) == fill_waiting_queries(expected)
    expected = {
            'queries': {1: {'query': 1, 'runtime': 0.2, 'status': 'ERROR', 'stream': 3}},
            'ok_count': 0, 'error_count': 1, 'timeout_count': 0
    }
    assert fixture.tpch_stats_for_stream_id(3) == fill_waiting_queries(expected)
    # yapf: enable

def test_tpch_totals():
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=1, tpcc_csv_interval=1)
    queue = MockQueue()
    fixture.process_queue(queue)
    assert fixture.tpch_totals() == (0, 0, 0)

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 3, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 3, 'status': 'ERROR', 'runtime': 0.1}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.0}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'TIMEOUT', 'runtime': 10.0}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=1, tpcc_csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    assert fixture.tpch_totals() == (1, 1, 1)

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'ERROR', 'runtime': 0.2}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.3}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'TIMEOUT', 'runtime': 11.3}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=1, tpcc_csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    assert fixture.tpch_totals() == (1, 1, 1)

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.0}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'TIMEOUT', 'runtime': 14.5}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'ERROR', 'runtime': 0.3}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'OK', 'runtime': 7.0}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'ERROR', 'runtime': 0.4}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=4, tpcc_csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    assert fixture.tpch_totals() == (2, 2, 1)
