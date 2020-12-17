
import glob
import os
import re
import shutil
import threading
import time
import random

from sys import exit
from traceback import print_tb

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from jinja2 import Environment, FileSystemLoader
from packaging.version import Version
from pathlib import Path
from psycopg2 import ProgrammingError, errors
from sqlparse import split as sqlparse_split
from subprocess import Popen, PIPE
from urllib.parse import urlparse

from .dbconn import DBConn

s64_benchmark_toolkit_root_dir = Path(os.path.abspath(__file__)).parents[1]

class NoIngestException(Exception):
    """Exception when no rows have been inserted into the table."""

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
    PYTHON_LOADER = False
    DO_SHUFFLE = True

    def __init__(self, args, benchmark):
        schemas_dir = os.path.join(s64_benchmark_toolkit_root_dir, benchmark.base_dir, "schemas")
        self.args = args
        self.benchmark = benchmark
        self.schema_dir = os.path.join(schemas_dir, args.schema)
        self.data_dir = args.data_dir
        self.cancel_event = threading.Event()
        self.num_partitions = args.num_partitions
        assert os.path.isdir(self.schema_dir), \
            f'Schema does not exist. Available ones are subfolders in {schemas_dir}'

    @property
    def swarm64da_version(self):
        try:
            with DBConn(self.args.dsn) as conn:
                conn.cursor.execute('SELECT swarm64da.get_version()')
                result = conn.cursor.fetchone()[0]

            s64da_version_string = re.findall(r'[0-9]+\.[0-9]+\.[0-9]+', result)[0]
            return Version(s64da_version_string)

        except ProgrammingError:
            return None

    @property
    def supports_cluster(self):
        version = self.swarm64da_version
        if not version:
            return False

        if self.num_partitions:
            print('Swarm64 DA CLUSTER not supported for partitioned schemas at the moment. Skipping')
            return False

        # Clustering not supported in S64 DA on native tables
        if "native" in self.schema_dir:
            print('Swarm64 DA CLUSTER not supported for S64 DA with Native Tables. Skipping')
            return False

        if version < Version('4.1') or version >= Version('5.0'):
            print('Swarm64 DA version does not support clustering. Skipping')
            return False

        return True

    def psql_exec_file(self, filename):
        return f'psql {self.args.dsn} -f {filename}'

    def psql_exec_cmd(self, sql):
        return f'psql {self.args.dsn} -c "{sql}"'

    @staticmethod
    def check_ingest(output):
        if output is not None and output.startswith("COPY"):
            cnt = int(output.strip().split()[1])
            if cnt == 0:
                raise NoIngestException("Ingest failed.")


    def _run_shell_task(self, task, return_output=False):
        if not self.cancel_event.is_set():
            p = Popen(task, cwd=self.benchmark.base_dir, shell=True, executable='/bin/bash',
                      stdout=PIPE if return_output else None)
            p.wait()
            if p.returncode != 0:
                self.cancel_event.set()
                exit(task)

            if return_output:
                stdout, _ = p.communicate()
                print(stdout.decode('utf-8'), end='')
                return stdout.decode('utf-8')

    def _run_tasks_parallel(self, tasks, executor_class=ThreadPoolExecutor):
        def get_runnable_task(task):
            if isinstance(task, tuple):
                task, task_args = (task[0], task[1:])

            if callable(task):
                return (task, *task_args)
            else:
                return (self._run_shell_task, task, True)

        def get_future(task):
            runnable = get_runnable_task(task)
            return executor.submit(runnable[0], *runnable[1:])

        def ingest_succeeded(task_result):
            try:
                PrepareBenchmarkFactory.check_ingest(task_result)
            except NoIngestException:
                return False

            return True

        # randomize the tasks to decrease lock contention
        if PrepareBenchmarkFactory.DO_SHUFFLE:
            random.shuffle(tasks)

        # If we're asked to use only with one job, run the tasks directly
        # in the main process to ease profiling.
        if self.args.max_jobs == 1:
            for task in tasks:
                runnable = get_runnable_task(task)
                result = runnable[0](*runnable[1:]) if len(runnable) > 1 else runnable[0]()
                if not ingest_succeeded(result):
                    print('Ingest failed!')
                    exit(1)
        else:
            with executor_class(max_workers=self.args.max_jobs) as executor:
                futures = [get_future(task) for task in tasks]
                for completed_future in as_completed(futures):
                    ingest_failed = not ingest_succeeded(completed_future.result())
                    exc = completed_future.exception()
                    if exc:
                        print(f'Task threw an exception: {exc}')
                        print_tb(exc.__traceback__)
                    if exc or ingest_failed:
                            for future in futures:
                                future.cancel()
                            print('Ingest failed!')
                            exit(1)

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

    def run(self):
        diskpace_check_dir = self.args.check_diskspace_of_directory
        if diskpace_check_dir:
            self._check_diskspace(diskpace_check_dir)

        print('Preparing DB')
        self.prepare_db()

        print('Ingesting data')
        self.cancel_event.clear()
        start_ingest = time.time()
        for table_group in PrepareBenchmarkFactory.TABLES:
            ingest_tasks = []
            for table in table_group:
                tasks = self.get_ingest_tasks(table)
                assert isinstance(tasks, list), 'Returned object is not a list'
                ingest_tasks.extend(tasks)

            if PrepareBenchmarkFactory.PYTHON_LOADER:
                self._run_tasks_parallel(ingest_tasks, executor_class=ProcessPoolExecutor)
            else:
                self._run_tasks_parallel(ingest_tasks)

        ingest_duration = time.time() - start_ingest

        print('Adding indices')
        start_optimize = time.time()
        self.add_indexes()

        print('Adding common')
        self.add_common()

        if self.supports_cluster:
            print('Swarm64 DA CLUSTER')
            self.cluster()

        print('VACUUM-ANALYZE')
        self.vacuum_analyze()
        optimize_duration = time.time() - start_optimize

        with open("prepare_metrics.csv", "w") as prepare_metrics_file:
            prepare_metrics_file.write(f'ingest; {ingest_duration}\n')
            prepare_metrics_file.write(f'optimize; {optimize_duration}')


        print(f'Process complete. DSN: {self.args.dsn}')

    def _load_pre_schema(self, conn):
        pre_schema_path = os.path.join(self.schema_dir, 'pre_schema.sql')
        if os.path.isfile(pre_schema_path):
            print(f'Loading pre-schema {pre_schema_path}')
            with open(pre_schema_path, 'r') as pre_schema_file:
                conn.cursor.execute(pre_schema_file.read())

    def _load_license(self, conn):
        license_loaded = True

        try:
            conn.cursor.execute(f'select swarm64da.show_license()')
        except errors.UndefinedFunction as err:
            print('License check function not found. Skipping, presumably on AWS.')
            return
        except errors.InternalError:
            license_loaded = False

        if not license_loaded:
            try:
                license_path = self.args.s64da_license_path
                print(f'Loading license from: {license_path}')
                conn.cursor.execute(f'select swarm64da.load_license(\'{license_path}\')')
            except errors.InternalError:
                print(f'Could not load S64 DA license file or file is invalid: {license_path}\n'
                      f'Make sure the --s64da-license-path argument points to a valid license file.')
                raise
            except IndexError as err:
                print(f'S64 DA licensing error: {err}')
                raise

        try:
            conn.cursor.execute(f'select swarm64da.show_license()')
            license_status = conn.cursor.fetchall()[0]
            print(f'S64 DA license status: {license_status}')
        except Exception as err:
            print(f'Error reading S64 DA license: {err}')
            raise

    def _load_schema(self, conn, applied_schema_path):
        print(f'Loading schema {applied_schema_path}')
        with open(applied_schema_path, 'r') as schema:
            schema_sql = schema.read()
            conn.cursor.execute(schema_sql)

    def prepare_db(self):
        dsn_url = urlparse(self.args.dsn)
        dbname = dsn_url.path[1:]

        with DBConn(f'{dsn_url.scheme}://{dsn_url.netloc}/postgres') as conn:
            print(f'Deleting Database {dbname} if it already exists')
            conn.cursor.execute(f'DROP DATABASE IF EXISTS {dbname}')
            print(f'Creating Database {dbname}')
            conn.cursor.execute(f"CREATE DATABASE {dbname} TEMPLATE template0 ENCODING 'UTF-8'")


        applied_schema_path = os.path.join(s64_benchmark_toolkit_root_dir, 'applied_schema.sql')

        jinja_env = Environment(loader=FileSystemLoader(self.schema_dir))
        applied_schema = jinja_env.get_template("schema.sql").render(num_partitions=self.num_partitions)

        with open(applied_schema_path, "w") as applied_schema_file:
            applied_schema_file.write(applied_schema)

        with DBConn(self.args.dsn) as conn:
            print('Adding helper functions.')
            common_file_path = os.path.join(s64_benchmark_toolkit_root_dir, 'benchmarks', 'common', 'functions.sql')
            with open(common_file_path, 'r') as common_sql:
                conn.cursor.execute(common_sql.read())

            self._load_pre_schema(conn)

            if self.swarm64da_version:
                self._load_license(conn)

            self._load_schema(conn, applied_schema_path)

    def get_ingest_tasks(self, table):
        return []

    def add_indexes(self):
        for sql_file in ('primary-keys.sql', 'foreign-keys.sql', 'indexes.sql', 's64da_indexes.sql'):
            sql_file_path = os.path.join(s64_benchmark_toolkit_root_dir, self.schema_dir, sql_file)
            if not os.path.isfile(sql_file_path):
                continue

            print(f'Applying {sql_file_path}')
            with open(sql_file_path, 'r') as sql_file:
                sql = sql_file.read()

            tasks = [self.psql_exec_cmd(cmd) for cmd in sqlparse_split(sql)]
            self._run_tasks_parallel(tasks)

    def add_common(self):
        common_path = os.path.join(self.schema_dir, '..', 'common', '*.sql')
        tasks = [self.psql_exec_file(sql_file) for sql_file in glob.glob(common_path)]
        self._run_tasks_parallel(tasks)

    def vacuum_analyze(self):
        print(f'Running VACUUM-ANALYZE on {self.args.dsn}')

        vacuum_tasks = []
        analyze_tasks = []
        tables = PrepareBenchmarkFactory.TABLES_ANALYZE or PrepareBenchmarkFactory.TABLES
        for table_group in tables:
            for table in table_group:
                vacuum_tasks.append(self.psql_exec_cmd(f'VACUUM {table}'))
                analyze_tasks.append(self.psql_exec_cmd(f'ANALYZE {table}'))

        self._run_tasks_parallel(vacuum_tasks)
        self._run_tasks_parallel(analyze_tasks)

    def cluster(self):
        cluster_tasks = [
                self.psql_exec_cmd(f"SELECT swarm64da.cluster('{table}', '{colspec}')")
                for table, colspec in PrepareBenchmarkFactory.CLUSTER_SPEC.items()]
        self._run_tasks_parallel(cluster_tasks)
