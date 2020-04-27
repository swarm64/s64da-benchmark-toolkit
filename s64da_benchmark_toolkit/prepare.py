
import os
import re
import shutil

from collections import namedtuple

from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2 import ProgrammingError
from subprocess import Popen, PIPE
from urllib.parse import urlparse

from .dbconn import DBConn

Swarm64DAVersion = namedtuple('Swarm64DAVersion', ['major', 'minor', 'patch'])

class TableGroup:
    def __init__(self, *args):
        self.data = args
        self._idx = 0

    def __iter__(self):
        self._idx = 0
        return self

    def __next__(self):
        if self._idx == len(self.data):
            raise StopIteration
        self._idx += 1
        return self.data[self._idx - 1]


class PrepareBenchmarkFactory:
    TABLES = []
    TABLES_ANALYZE = None
    SIZING_FACTORS = {}
    CLUSTER_SPEC = {}

    def __init__(self, args, benchmark):
        self.args = args
        self.benchmark = benchmark
        self.schema_dir = os.path.join(benchmark.base_dir, 'schemas', args.schema)
        self.data_dir = args.data_dir
        assert os.path.isdir(self.schema_dir), 'Schema does not exist'

    @property
    def swarm64da_version(self):
        try:
            with DBConn(self.args.dsn) as conn:
                conn.cursor.execute('SELECT swarm64da.get_version()')
                result = conn.cursor.fetchone()[0]

            s64da_version_string = re.findall(r'[0-9]+\.[0-9]+\.[0-9]+', result)[0]
            return Swarm64DAVersion(*[int(part) for part in s64da_version_string.split('.')])

        except ProgrammingError:
            return None

    @property
    def supports_cluster(self):
        version = self.swarm64da_version
        if not version:
            return False

        if version.major <= 4 and version.minor < 1:
            return False

        return True

    def psql_exec_file(self, filename):
        return f'psql {self.args.dsn} -f {filename}'

    def psql_exec_cmd(self, sql):
        return f'psql {self.args.dsn} -c "{sql}"'

    def _run_shell_task(self, task, return_output=False):
        p = Popen(task, cwd=self.benchmark.base_dir, shell=True, executable='/bin/bash',
                  stdout=PIPE if return_output else None)
        p.wait()
        assert p.returncode == 0, 'Shell task did not finish with exit code 0'

        if return_output:
            stdout, _ = p.communicate()
            return stdout

    def _run_tasks_parallel(self, tasks):
        with ThreadPoolExecutor(max_workers=self.args.max_jobs) as executor:
            futures = [executor.submit(self._run_shell_task, task) for task in tasks]
            for completed_future in as_completed(futures):
                exc = completed_future.exception()
                if exc:
                    print(f'Task threw an exception: {exc}')
                    for future in futures:
                        future.cancel()

    def _check_diskspace(self, diskpace_check_dir):
        db_type = os.path.basename(self.schema_dir).split('_')[0]
        if db_type == 'sdb':
            db_type = 's64da'

        scale_factor = self.args.scale_factor
        size_factor = PrepareBenchmarkFactory.SIZING_FACTORS[db_type].get(scale_factor)
        if not size_factor:
            print(f'Could not determine size factor. Not checking disk space.')
            return

        print(f'Checking available diskpace in {diskpace_check_dir} with assumptions:\n'
              f'    Storage dir   : {diskpace_check_dir}\n'
              f'    Database type : {db_type}\n'
              f'    Benchmark     : {self.benchmark.name}\n'
              f'    Scale factor  : {scale_factor}\n'
              f'    Size factor   : {size_factor}')

        _, _, free = shutil.disk_usage(diskpace_check_dir)
        space_needed = int(scale_factor * size_factor) << 30
        assert space_needed < free, \
            f'Not enough disk space available. Needed [GBytes]: {space_needed>>30}, free: {free>>30}'

    def _apply_schema_modifications(self, schema_sql):
        version = self.swarm64da_version
        if not version:
            # No S64 DA, no modifications
            return schema_sql

        if not (version.major <= 4 and version.minor < 1):
            schema_sql = schema_sql.replace('optimized_columns', 'range_index')

        return schema_sql

    def run(self):
        diskpace_check_dir = self.args.check_diskspace_of_directory
        if diskpace_check_dir:
            self._check_diskspace(diskpace_check_dir)

        print('Preparing DB')
        self.prepare_db()

        print('Ingesting data')
        for table_group in PrepareBenchmarkFactory.TABLES:
            ingest_tasks = []
            for table in table_group:
                tasks = self.get_ingest_tasks(table)
                assert isinstance(tasks, list), 'Returned object is not a list'
                ingest_tasks.extend(tasks)
            self._run_tasks_parallel(ingest_tasks)

        print('Adding indices')
        self.add_indexes()

        if self.supports_cluster:
            print('Swarm64 DA CLUSTER')
            self.cluster()

        print('VACUUM-ANALYZE')
        self.vacuum_analyze()

    def _load_pre_schema(self, conn):
        pre_schema_path = os.path.join(self.schema_dir, 'pre_schema.sql')
        if os.path.isfile(pre_schema_path):
            print(f'Loading pre-schema')
            with open(pre_schema_path, 'r') as pre_schema_file:
                conn.cursor.execute(pre_schema_file.read())

    def _load_schema(self, conn):
        print(f'Loading schema')
        schema_path = os.path.join(self.schema_dir, 'schema.sql')
        with open(schema_path, 'r') as schema:
            schema_sql = schema.read()
            schema_sql = self._apply_schema_modifications(schema_sql)
            conn.cursor.execute(schema_sql)

    def prepare_db(self):
        dsn_url = urlparse(self.args.dsn)
        dbname = dsn_url.path[1:]

        with DBConn(f'{dsn_url.scheme}://{dsn_url.netloc}/postgres') as conn:
            print(f'Deleting {dbname}')
            conn.cursor.execute(f'DROP DATABASE IF EXISTS {dbname}')
            print(f'Creating {dbname}')
            conn.cursor.execute(f"CREATE DATABASE {dbname} TEMPLATE template0 ENCODING 'UTF-8'")

        with DBConn(self.args.dsn) as conn:
            self._load_pre_schema(conn)
            self._load_schema(conn)

    def get_ingest_tasks(self, table):
        return []

    def add_indexes(self):
        for sql_file in ('primary-keys.sql', 'foreign-keys.sql', 'indexes.sql'):
            sql_file_path = os.path.join(self.schema_dir, sql_file)
            if not os.path.isfile(sql_file_path):
                continue

            with DBConn(self.args.dsn) as conn:
                print(f'Applying {sql_file_path}')
                with open(sql_file_path, 'r') as sql_file:
                    sql = sql_file.read()
                    if sql:
                        conn.cursor.execute(sql)

    def vacuum_analyze(self):
        self._run_shell_task(f'psql {self.args.dsn} -c "VACUUM"')

        analyze_tasks = []
        tables = PrepareBenchmarkFactory.TABLES_ANALYZE or PrepareBenchmarkFactory.TABLES
        for table_group in tables:
            for table in table_group:
                analyze_tasks.append(f'psql {self.args.dsn} -c "ANALYZE {table}"')

        self._run_tasks_parallel(analyze_tasks)

    def cluster(self):
        cluster_tasks = [
                f'''psql {self.args.dsn} -c "SELECT swarm64da.cluster('{table}', '{colspec}')"'''
                for table, colspec in PrepareBenchmarkFactory.CLUSTER_SPEC.items()]
        self._run_tasks_parallel(cluster_tasks)
