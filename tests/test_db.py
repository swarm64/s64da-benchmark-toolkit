
from unittest.mock import call

import psycopg2
import pytest

from s64da_benchmark_toolkit import db


DSN = 'postgresql://postgres@nowhere:1234/foodb'
DSN_PG = 'postgresql://postgres@nowhere:1234/postgres'


def get_mocked_cursor(mocker):
    psycopg2_connect = mocker.patch('psycopg2.connect')
    mock_conn = psycopg2_connect.return_value
    return mock_conn.cursor.return_value


@pytest.fixture
def no_plan(monkeypatch):
    def monkeyplan(conn, sql):
        return None

    some_db = db.DB
    monkeypatch.setattr(some_db, "get_explain_output", monkeyplan)


def test_db_init():
    some_db = db.DB(DSN)
    assert some_db.dsn == DSN
    assert some_db.dsn_pg_db == DSN_PG


def test_db_apply_config(mocker):
    mock_cursor = get_mocked_cursor(mocker)

    db.DB(DSN).apply_config({
        'foo': 'bar',
        'bla': 1
    })

    mock_cursor.execute.assert_has_calls([
        call('ALTER SYSTEM SET foo = $$bar$$'),
        call('ALTER SYSTEM SET bla = $$1$$'),
        call('SELECT pg_reload_conf()')
    ])


def test_db_reset_config(mocker):
    mock_cursor = get_mocked_cursor(mocker)
    db.DB(DSN).reset_config()

    mock_cursor.execute.assert_has_calls([
        call('ALTER SYSTEM RESET ALL'),
        call('SELECT pg_reload_conf()')
    ])


def test_db_run_query_ok(mocker):
    mock_cursor = get_mocked_cursor(mocker)
    result, query_output = db.DB(DSN).run_query('SELECT 1', 0)

    assert result.status == db.Status.OK
    assert (result.stop - result.start) > 0
    assert ([], mock_cursor.fetchall()) == query_output
    mock_cursor.execute.assert_called_once_with('SELECT 1')


def test_db_run_query_timeout(no_plan, mocker):
    mock_cursor = get_mocked_cursor(mocker)
    mock_cursor.execute.side_effect = psycopg2.extensions.QueryCanceledError('Timeout')

    result, query_output = db.DB(DSN).run_query('SELECT 1', 0)
    assert result.status == db.Status.TIMEOUT
    assert (result.stop - result.start) > 0
    assert query_output is None
    mock_cursor.execute.assert_called_once_with('SELECT 1')


def test_db_run_query_error(no_plan, mocker):
    mock_cursor = get_mocked_cursor(mocker)
    mock_cursor.execute.side_effect = psycopg2.InternalError('Error')

    result, query_output = db.DB(DSN).run_query('SELECT 1', 0)
    assert result.status == db.Status.ERROR
    assert (result.stop - result.start) > 0
    assert query_output is None
    mock_cursor.execute.assert_called_once_with('SELECT 1')
