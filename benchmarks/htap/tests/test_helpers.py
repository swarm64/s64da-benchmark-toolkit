import pytest

from dateutil.parser import isoparse

from benchmarks.htap.lib.helpers import Random, TPCCText, TimestampGenerator


def is_alpha(c):
    return ((ord(c) >= ord('a') and ord(c) <= ord('z'))
            or (ord(c) >= ord('A') and ord(c) <= ord('Z')))


def is_special(c):
    return c == '?' or c == '@'


def test_random():
    fixture = Random(0)
    assert fixture.decision(1 / 2) == True
    assert fixture.decision(1 / 2) == False
    assert fixture.decision(1 / 2) == True
    assert fixture.decision(1 / 2) == False


def test_tpcc_text():
    fixture = TPCCText(Random(0))
    assert all([is_alpha(c) for c in fixture.string(100)])
    assert all([c.isdigit() for c in fixture.numstring(100)])
    assert all([is_alpha(c) or c.isdigit() for c in fixture.alnumstring(100)])
    assert all([is_alpha(c) or is_special(c) or c.isdigit() for c in fixture.alnum64string(100)])
    assert 'ORIGINAL' in fixture.data_original(0, 100)


def test_timestamp_distance():
    expected_distance = (isoparse('1998-08-02') - isoparse('1992-01-01')) * (0.44 / 1500000)

    assert TimestampGenerator.timestamp_distance(scale_factor=1) == expected_distance
    assert TimestampGenerator.timestamp_distance(scale_factor=10) == expected_distance / 10
    assert TimestampGenerator.timestamp_distance(scale_factor=100) == expected_distance / 100
    assert TimestampGenerator.timestamp_distance(scale_factor=1000) == expected_distance / 1000
