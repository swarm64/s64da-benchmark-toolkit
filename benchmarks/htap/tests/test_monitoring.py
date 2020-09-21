import pytest

from datetime import datetime

from benchmarks.htap.lib.monitoring import Stats


class MockQueue:
    def __init__(self, data=[]):
        self.data = data

    def empty(self):
        return len(self.data) == 0

    def get_nowait(self):
        return self.data.pop(0)


def same_stats(lhs, rhs):
    if 'worker_id' in rhs:
        rhs = rhs.copy()
        del rhs['worker_id']

    return lhs == rhs


def test_storage():
    now = datetime.now()
    fixture = Stats(num_oltp_slots=4, num_olap_slots=0)

    # yapf: disable
    data = [
            ('tpcc', { 'worker_id': 0, 'ok_count': 0, 'err_count': 100, 'new_order_count': 44 }),
            ('tpcc', { 'worker_id': 1, 'ok_count': 100, 'err_count': 0, 'new_order_count': 43 }),
            ('tpcc', { 'worker_id': 2, 'ok_count': 0, 'err_count': 0, 'new_order_count': 0 }),
            ('tpcc', { 'worker_id': 2, 'ok_count': 3, 'err_count': 0, 'new_order_count': 1 }),
            ('tpcc', { 'worker_id': 3, 'ok_count': 100, 'err_count': 100, 'new_order_count': 87 }),
            ('tpcc', { 'worker_id': 0, 'ok_count': 0, 'err_count': 97, 'new_order_count': 42 }),
    ]
    # yapf: enable
    queue = MockQueue(data.copy())
    fixture.update(queue, now)
    # Newer values overwrite older values
    assert same_stats(fixture.tpcc_stats_for_stream_id(0), data[5][1])
    assert same_stats(fixture.tpcc_stats_for_stream_id(1), data[1][1])
    assert same_stats(fixture.tpcc_stats_for_stream_id(2), data[3][1])
    assert same_stats(fixture.tpcc_stats_for_stream_id(3), data[4][1])

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Finished', 'runtime': 1.0}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Canceled'}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Error'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Finished', 'runtime': 7.0}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Error'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Finished', 'runtime': 2.3}),
    ]
    # yapf: enable
    fixture = Stats(num_oltp_slots=0, num_olap_slots=4)
    queue = MockQueue(data)
    fixture.update(queue, 0)
    # Newer values extend older values (except for query runtimes)
    # yapf: disable
    expected = {
            'queries': {1: 2.3, 7: 7.0}, 'finished_count': 3, 'error_count': 0, 'timeout_count': 0
    }
    assert fixture.tpch_stats_for_stream_id(0) == expected
    expected = {
            'queries': {3: 'Error'}, 'finished_count': 0, 'error_count': 1, 'timeout_count': 0
    }
    assert fixture.tpch_stats_for_stream_id(1) == expected
    expected = {
            'queries': {9: 'Timeout'}, 'finished_count': 0, 'error_count': 0, 'timeout_count': 1
    }
    assert fixture.tpch_stats_for_stream_id(2) == expected
    expected = {
            'queries': {1: 'Error'}, 'finished_count': 0, 'error_count': 1, 'timeout_count': 0
    }
    assert fixture.tpch_stats_for_stream_id(3) == expected
    # yapf: enable


def test_tpcc_totals():
    now = datetime.now()
    fixture = Stats(num_oltp_slots=1, num_olap_slots=0)
    queue = MockQueue()
    fixture.update(queue, now)
    assert fixture.tpcc_totals() == (0, 0, 0)
    assert fixture.tpch_totals() == (0, 0, 0)

    # yapf: disable
    data = [( 'tpcc', { 'worker_id': 0, 'ok_count': 100, 'err_count': 10, 'new_order_count': 4 })]
    # yapf: enable
    queue = MockQueue(data)
    fixture.update(queue, now)
    assert fixture.tpcc_totals() == (100, 10, 4)
    assert fixture.tpch_totals() == (0, 0, 0)

    fixture = Stats(num_oltp_slots=4, num_olap_slots=0)
    # yapf: disable
    data = [
            ('tpcc', { 'worker_id': 0, 'ok_count': 0, 'err_count': 100, 'new_order_count': 44 }),
            ('tpcc', { 'worker_id': 1, 'ok_count': 100, 'err_count': 0, 'new_order_count': 43 }),
            ('tpcc', { 'worker_id': 2, 'ok_count': 0, 'err_count': 0, 'new_order_count': 0 }),
            ('tpcc', { 'worker_id': 3, 'ok_count': 100, 'err_count': 100, 'new_order_count': 87 })
    ]
    # yapf: enable
    queue = MockQueue(data)
    fixture.update(queue, now)
    assert fixture.tpcc_totals() == (100 + 100, 100 + 100, 44 + 43 + 87)
    assert fixture.tpch_totals() == (0, 0, 0)

    fixture = Stats(num_oltp_slots=2, num_olap_slots=0)
    # yapf: disable
    data = [
            ('tpcc', { 'worker_id': 0, 'ok_count': 12, 'err_count': 1, 'new_order_count': 5 }),
            ('tpcc', { 'worker_id': 1, 'ok_count': 100, 'err_count': 2, 'new_order_count': 45 }),
            ('tpcc', { 'worker_id': 0, 'ok_count': 23, 'err_count': 4, 'new_order_count': 44 }),
    ]
    # yapf: enable
    queue = MockQueue(data)
    fixture.update(queue, now)
    assert fixture.tpcc_totals() == (100 + 23, 2 + 4, 45 + 44)
    assert fixture.tpch_totals() == (0, 0, 0)


def test_tpch_totals():
    fixture = Stats(num_oltp_slots=0, num_olap_slots=1)
    queue = MockQueue()
    fixture.update(queue, 0)
    assert fixture.tpcc_totals() == (0, 0, 0)
    assert fixture.tpch_totals() == (0, 0, 0)

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 3, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 3, 'status': 'Error'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Finished', 'runtime': 1.0}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Canceled'}),
    ]
    # yapf: enable
    fixture = Stats(num_oltp_slots=0, num_olap_slots=1)
    queue = MockQueue(data)
    fixture.update(queue, 0)
    assert fixture.tpcc_totals() == (0, 0, 0)
    assert fixture.tpch_totals() == (1, 1, 1)

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Error'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Finished', 'runtime': 1.3}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Canceled'}),
    ]
    # yapf: enable
    fixture = Stats(num_oltp_slots=0, num_olap_slots=1)
    queue = MockQueue(data)
    fixture.update(queue, 0)
    assert fixture.tpcc_totals() == (0, 0, 0)
    assert fixture.tpch_totals() == (1, 1, 1)

    # yapf: disable
    data = [
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Finished', 'runtime': 1.0}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Canceled'}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Error'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Finished', 'runtime': 7.0}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Error'}),
    ]
    # yapf: enable
    fixture = Stats(num_oltp_slots=0, num_olap_slots=4)
    queue = MockQueue(data)
    fixture.update(queue, 0)
    assert fixture.tpcc_totals() == (0, 0, 0)
    assert fixture.tpch_totals() == (2, 2, 1)


def test_mixed_totals():
    # yapf: disable
    data = [
            ('tpcc', { 'worker_id': 0, 'ok_count': 0, 'err_count': 100, 'new_order_count': 44 }),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 1, 'status': 'Finished', 'runtime': 1.0}),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Running'}),
            ('tpch', {'stream': 2, 'query': 9, 'status': 'Canceled'}),
            ('tpcc', { 'worker_id': 1, 'ok_count': 100, 'err_count': 0, 'new_order_count': 43 }),
            ('tpcc', { 'worker_id': 2, 'ok_count': 0, 'err_count': 0, 'new_order_count': 0 }),
            ('tpch', {'stream': 1, 'query': 3, 'status': 'Error'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Running'}),
            ('tpch', {'stream': 0, 'query': 7, 'status': 'Finished', 'runtime': 7.0}),
            ('tpcc', { 'worker_id': 0, 'ok_count': 100, 'err_count': 100, 'new_order_count': 87 }),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Running'}),
            ('tpch', {'stream': 3, 'query': 1, 'status': 'Error'}),
    ]
    # yapf: enable
    fixture = Stats(num_oltp_slots=4, num_olap_slots=4)
    queue = MockQueue(data)
    fixture.update(queue, 0)
    assert fixture.tpcc_totals() == (200, 100, 130)
    assert fixture.tpch_totals() == (2, 2, 1)


def test_comsume_stats():
    now = datetime.now()
    fixture = Stats(num_oltp_slots=4, num_olap_slots=0)
    # yapf: disable
    data = [
            ('tpcc', { 'worker_id': 0, 'ok_count': 0, 'err_count': 99, 'new_order_count': 44 }),
            ('tpcc', { 'worker_id': 1, 'ok_count': 100, 'err_count': 0, 'new_order_count': 43 }),
            ('tpcc', { 'worker_id': 2, 'ok_count': 0, 'err_count': 0, 'new_order_count': 0 }),
            ('tpcc', { 'worker_id': 3, 'ok_count': 100, 'err_count': 98, 'new_order_count': 87 })
    ]
    # yapf: enable
    queue = MockQueue(data)
    fixture.update(queue, now)
    assert fixture.tpcc_totals() == (100 + 100, 99 + 98, 44 + 43 + 87)
    assert fixture.tpch_totals() == (0, 0, 0)

    tpcc_ok_sum, tpcc_ok_rate, tpcc_err_sum, tpcc_err_rate = fixture.consume_stats()
    assert(tpcc_ok_sum == 100 + 100)
    assert(tpcc_ok_rate == 100 + 100)
    assert(tpcc_err_sum == 99 + 98)
    assert(tpcc_err_rate == 99 + 98)

    # Rate stats have been consumed
    tpcc_ok_sum, tpcc_ok_rate, tpcc_err_sum, tpcc_err_rate = fixture.consume_stats()
    assert(tpcc_ok_sum == 100 + 100)
    assert(tpcc_ok_rate == 0)
    assert(tpcc_err_sum == 99 + 98)
    assert(tpcc_err_rate == 0)

    # yapf: disable
    data = [
            ('tpcc', { 'worker_id': 0, 'ok_count': 12, 'err_count': 101, 'new_order_count': 44 }),
            ('tpcc', { 'worker_id': 1, 'ok_count': 100, 'err_count': 2, 'new_order_count': 45 }),
            ('tpcc', { 'worker_id': 0, 'ok_count': 23, 'err_count': 107, 'new_order_count': 47 }),
    ]
    # yapf: enable
    queue = MockQueue(data)
    fixture.update(queue, now)
    # Data for provided streams has been replaced, for other streams it's untouched
    assert fixture.tpcc_totals() == (100 + 100 + 23, 98 + 2 + 107, 87 + 45 + 47)
    assert fixture.tpch_totals() == (0, 0, 0)

    tpcc_ok_sum, tpcc_ok_rate, tpcc_err_sum, tpcc_err_rate = fixture.consume_stats()
    assert tpcc_ok_sum == 100 + 100 + 23
    assert tpcc_ok_rate == 23
    assert tpcc_err_sum == 98 + 2 + 107
    assert tpcc_err_rate == 10
