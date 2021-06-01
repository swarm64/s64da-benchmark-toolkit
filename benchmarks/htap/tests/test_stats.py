import pytest

from datetime import datetime

from benchmarks.htap.lib.stats import Stats, QUERY_TYPES


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
    fixture = Stats(dsn, num_oltp_slots=4, num_olap_slots=0, csv_interval=1, history_length = 10, initial_sec = 0)

    # yapf: disable
    data = [
            # this will be not be kept
            ('oltp', [{ 'timestamp': 0, 'query': QUERY_TYPES[0], 'status':'ok', 'runtime': 0.01 }]),
            # from here on we have a 10s window
            ('oltp', [{ 'timestamp': 1.1, 'query': QUERY_TYPES[0], 'status':'ok', 'runtime': 0.1 }]),
            ('oltp', [{ 'timestamp': 2, 'query': QUERY_TYPES[1], 'status':'ok', 'runtime': 0.02 }]),
            ('oltp', [{ 'timestamp': 2, 'query': QUERY_TYPES[2], 'status':'ok', 'runtime': 0.03 }]),
            ('oltp', [{ 'timestamp': 2, 'query': QUERY_TYPES[3], 'status':'error', 'runtime': 0.04 }]),
            ('oltp', [{ 'timestamp': 2.1, 'query': QUERY_TYPES[0], 'status':'ok', 'runtime': 0.1 }]),
            ('oltp', [{ 'timestamp': 3.1, 'query': QUERY_TYPES[0], 'status':'ok', 'runtime': 0.1 }]),
            ('oltp', [{ 'timestamp': 3.2, 'query': QUERY_TYPES[0], 'status':'ok', 'runtime': 0.1 }]),
            ('oltp', [{ 'timestamp': 11, 'query': QUERY_TYPES[0], 'status':'ok', 'runtime': 1 }]),
                ]
    # yapf: enable
    queue = MockQueue(data.copy())
    fixture.process_queue(queue)

    # test:

    # check counters
    res = [fixture.oltp_counts(i) for i in QUERY_TYPES]
    res_all = fixture.oltp_counts()
    assert res[0] == (6, 0)
    assert res[1] == (1, 0)
    assert res[2] == (1, 0)
    assert res[3] == (0, 1)
    assert res_all == (8, 1)

    # check tps and latency
    res = [fixture.oltp_total(i) for i in QUERY_TYPES]
    res_all = fixture.oltp_total()

    # tps
    assert res[0][0] == (0, 0, 0, 2)
    assert res[1][0] == (0, 0, 0, 1)
    assert res[2][0] == (0, 0, 0, 1)
    assert res[3][0] == (0, 0, 0, 0)
    assert res_all[0] == (0, 0, 0, 3)

    # latencies
    assert res[0][1] == (0, 100, int((100+100+100+1000)/4), 1000)
    assert res[1][1] == (0, 20, 20, 20)
    assert res[2][1] == (0, 30, 30, 30)
    assert res[3][1] == (0, 40, 40, 40)
    assert res_all[1] == (0, 20, int((20+30+40+100+100+100+1000)/7), 1000)
    
    # yapf: disable
    data = [
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'TIMEOUT', 'runtime': 10.0}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.0}),
            ('olap', {'stream': 1, 'query': 3, 'status': 'Running'}),
            ('olap', {'stream': 2, 'query': 9, 'status': 'Running'}),
            ('olap', {'stream': 2, 'query': 9, 'status': 'TIMEOUT', 'runtime': 13.5}),
            ('olap', {'stream': 1, 'query': 3, 'status': 'ERROR', 'runtime': 0.1}),
            ('olap', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 7, 'status': 'OK', 'runtime': 7.0}),
            ('olap', {'stream': 3, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 3, 'query': 1, 'status': 'ERROR', 'runtime': 0.2}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 2.3}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=4, csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    # Newer values extend older values (except for query runtimes)
    # yapf: disable
    expected = {
            'queries': {1: {'query': 1, 'runtime': 2.3, 'status': 'OK', 'stream': 0}, 
                        7: {'query': 7, 'runtime': 7.0, 'status': 'OK', 'stream': 0},
                       },
            'ok_count': 3, 'error_count': 0, 'timeout_count': 1, 'ignored_count': 0
    }
    assert fixture.olap_stats_for_stream_id(0) == fill_waiting_queries(expected)
    expected = {
            'queries': {3: {'query': 3, 'runtime': 0.1, 'status': 'ERROR', 'stream': 1}},
            'ok_count': 0, 'error_count': 1, 'timeout_count': 0, 'ignored_count': 0
    }
    assert fixture.olap_stats_for_stream_id(1) == fill_waiting_queries(expected)
    expected = {
            'queries': {9: {'query': 9, 'runtime': 13.5, 'status': 'TIMEOUT', 'stream': 2}},
            'ok_count': 0, 'error_count': 0, 'timeout_count': 1, 'ignored_count': 0
    }
    assert fixture.olap_stats_for_stream_id(2) == fill_waiting_queries(expected)
    expected = {
            'queries': {1: {'query': 1, 'runtime': 0.2, 'status': 'ERROR', 'stream': 3}},
            'ok_count': 0, 'error_count': 1, 'timeout_count': 0, 'ignored_count': 0
    }
    assert fixture.olap_stats_for_stream_id(3) == fill_waiting_queries(expected)
    # yapf: enable

def test_olap_totals():
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=1, csv_interval=1)
    queue = MockQueue()
    fixture.process_queue(queue)
    assert fixture.olap_totals() == (0, 0, 0)

    # yapf: disable
    data = [
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 3, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 3, 'status': 'ERROR', 'runtime': 0.1}),
            ('olap', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.0}),
            ('olap', {'stream': 0, 'query': 7, 'status': 'TIMEOUT', 'runtime': 10.0}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=1, csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    assert fixture.olap_totals() == (1, 1, 1)

    # yapf: disable
    data = [
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'ERROR', 'runtime': 0.2}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.3}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'TIMEOUT', 'runtime': 11.3}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=1, csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    assert fixture.olap_totals() == (1, 1, 1)

    # yapf: disable
    data = [
            ('olap', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 1, 'status': 'OK', 'runtime': 1.0}),
            ('olap', {'stream': 1, 'query': 3, 'status': 'Running'}),
            ('olap', {'stream': 2, 'query': 9, 'status': 'Running'}),
            ('olap', {'stream': 2, 'query': 9, 'status': 'TIMEOUT', 'runtime': 14.5}),
            ('olap', {'stream': 1, 'query': 3, 'status': 'ERROR', 'runtime': 0.3}),
            ('olap', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('olap', {'stream': 0, 'query': 7, 'status': 'OK', 'runtime': 7.0}),
            ('olap', {'stream': 3, 'query': 1, 'status': 'Running'}),
            ('olap', {'stream': 3, 'query': 1, 'status': 'ERROR', 'runtime': 0.4}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=4, csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    assert fixture.olap_totals() == (2, 2, 1)

def test_olap_stream_totals():
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=1, csv_interval=1)
    queue = MockQueue()
    fixture.process_queue(queue)
    assert fixture.olap_stream_totals() == ('-', 0)

    # yapf: disable
    data = [
            ('olap_stream', {'stream': 0, 'iteration': 0, 'runtime': '1'}),
            ('olap_stream', {'stream': 1, 'iteration': 0, 'runtime': '10'}),
            ('olap_stream', {'stream': 2, 'iteration': 0, 'runtime': '20'}),
            ('olap_stream', {'stream': 0, 'iteration': 1, 'runtime': '1'}),
    ]
    # yapf: enable
    fixture = Stats(dsn, num_oltp_slots=0, num_olap_slots=3, csv_interval=1)
    queue = MockQueue(data)
    fixture.process_queue(queue)
    assert fixture.olap_stream_totals() == (8, 4)
