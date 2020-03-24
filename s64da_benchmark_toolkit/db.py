
import json
import logging
import time

from collections import namedtuple
from enum import Enum
from urllib.parse import urlparse

from .dbconn import DBConn

import psycopg2

LOG = logging.getLogger()
Timing = namedtuple('Timing', ['start', 'stop', 'status'])


class Status(Enum):
    OK = 0
    TIMEOUT = 1
    ERROR = 2


class DB:
    def __init__(self, dsn):
        self.dsn = dsn
        dsn_url = urlparse(dsn)
        self.dsn_pg_db = f'{dsn_url.scheme}://{dsn_url.netloc}/postgres'
        self.plan = ''

    def apply_config(self, config):
        with DBConn(self.dsn_pg_db) as conn:
            for key, value in config.items():
                conn.cursor.execute(f'ALTER SYSTEM SET {key} = $${value}$$')

            conn.cursor.execute('SELECT pg_reload_conf()')

    def reset_config(self):
        with DBConn(self.dsn_pg_db) as conn:
            conn.cursor.execute('ALTER SYSTEM RESET ALL')
            conn.cursor.execute('SELECT pg_reload_conf()')

    def run_query(self, sql, timeout, auto_explain=False):
        status = Status.ERROR
        query_result = None
        with DBConn(self.dsn, statement_timeout=timeout) as conn:
            try:
                start = time.time()

                if auto_explain:
                    DB.auto_explain_on(conn)

                conn.cursor.execute(sql)
                if conn.cursor.description is not None:
                    query_result_columns = [colname[0] for colname in conn.cursor.description]
                    query_result = query_result_columns, conn.cursor.fetchall()
                else:
                    query_result = None
                status = Status.OK
                self.plan = '\n'.join(conn.conn.notices)

            except psycopg2.extensions.QueryCanceledError:
                status = Status.TIMEOUT
                query_result = None
                self.plan = DB.get_explain_output(conn.conn, sql)

            except (psycopg2.InternalError, psycopg2.Error, UnicodeDecodeError):
                LOG.exception('Ignoring psycopg2 Error')
                query_result = None
                self.plan = DB.get_explain_output(conn.conn, sql)

            finally:
                stop = time.time()

            return Timing(start=start, stop=stop, status=status), query_result

    @staticmethod
    def auto_explain_on(conn):
        auto_explain_config = {
            'auto_explain.log_min_duration': 500,
            'auto_explain.log_analyze': 'on',
            'auto_explain.log_buffers': 'on',
            'auto_explain.log_format': 'json',
            'client_min_messages': 'LOG'
        }

        conn.cursor.execute("LOAD 'auto_explain'")

        for key, value in auto_explain_config.items():
            conn.cursor.execute(f'SET {key} = $${value}$$')

    @staticmethod
    def get_explain_output(connection, sql):
        try:

            with connection.cursor() as explain_plan_cursor:
                explain_plan_cursor.execute(sql.replace('-- EXPLAIN (FORMAT JSON)', 'EXPLAIN (FORMAT JSON)'))
                return json.dumps(explain_plan_cursor.fetchone()[0], indent=4)

        except psycopg2.Error as e:
            return f'{{"Explain Output failed": "{str(e)}"}}'
