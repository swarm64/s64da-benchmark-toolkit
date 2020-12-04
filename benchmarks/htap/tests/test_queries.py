import pytest

from datetime import datetime
from dateutil.parser import isoparse

from benchmarks.htap.lib.queries import Queries


def test_tpch_date_to_benchmark_date():
    now = datetime.now()
    tpch_max_date = isoparse('1998-12-01')

    tpch_date = isoparse('1998-12-01')
    assert Queries.tpch_date_to_benchmark_date(tpch_max_date, now) == now

    tpch_date = isoparse('1995-12-01')
    now = isoparse('2020-11-01')
    three_years_ago = isoparse('2017-11-01')
    assert Queries.tpch_date_to_benchmark_date(tpch_date, now) == three_years_ago

    tpch_date = isoparse('2007-11-01')
    now = isoparse('2020-11-01')
    expected = now - (tpch_max_date - tpch_date)
    assert Queries.tpch_date_to_benchmark_date(tpch_date, now) == expected
