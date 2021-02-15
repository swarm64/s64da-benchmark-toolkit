import pytest

from datetime import datetime
from dateutil.parser import isoparse

from benchmarks.htap.lib.queries import Queries


def test_tpch_date_to_benchmark_date():
    tpch_min_date = isoparse('1992-01-01')
    tpch_max_date = isoparse('1998-12-31')

    # date ranges that are in the same range should get the same result
    date1 = isoparse('1993-01-01')
    date2 = isoparse('1995-01-01')
    date3 = isoparse('2011-04-05')
    date4 = isoparse('1900-03-06')
    assert Queries.tpch_date_to_benchmark_date(date1, tpch_min_date, tpch_max_date) == date1
    assert Queries.tpch_date_to_benchmark_date(date2, tpch_min_date, tpch_max_date) == date2
    assert Queries.tpch_date_to_benchmark_date(date3, tpch_min_date, tpch_max_date) == date3
    assert Queries.tpch_date_to_benchmark_date(date4, tpch_min_date, tpch_max_date) == date4

