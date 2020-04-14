
import os
import shutil

from concurrent.futures import ThreadPoolExecutor, as_completed
from subprocess import Popen, PIPE
from urllib.parse import urlparse

from .dbconn import DBConn


class PrepareBenchmarkFactory:
    TABLES = []
    SIZING_FACTORS = {}
    CLUSTER_SPEC = {}

    def __init__(self, args, benchmark):
        self.args = args
        self.benchmark = benchmark
        self.schema_dir = os.path.join(benchmark.base_dir, 'schemas', args.schema)
        assert os.path.isdir(self.schema_dir), 'Schema does not exist'

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

    def run(self):
        diskpace_check_dir = self.args.check_diskspace_of_directory
        if diskpace_check_dir:
            self._check_diskspace(diskpace_check_dir)

        print('Preparing DB')
        self.prepare_db()

        print('Ingesting data')
        ingest_tasks = []
        for table in PrepareBenchmarkFactory.TABLES:
            tasks = self.get_ingest_tasks(table)
            assert isinstance(tasks, list), 'Returned object is not a list'
            ingest_tasks.extend(tasks)
        self._run_tasks_parallel(ingest_tasks)

        print('Adding indices')
        self.add_indexes()

        if self.supports_cluster():
            print('Swarm64 DA CLUSTER')
            self.cluster()

        print('VACUUM-ANALYZE')
        self.vacuum_analyze()

    def prepare_db(self):
        dsn_url = urlparse(self.args.dsn)
        dbname = dsn_url.path[1:]

        with DBConn(f'{dsn_url.scheme}://{dsn_url.netloc}/postgres') as conn:
            print(f'Deleting {dbname}')
            conn.cursor.execute(f'DROP DATABASE IF EXISTS {dbname}')
            print(f'Creating {dbname}')
            conn.cursor.execute(f"CREATE DATABASE {dbname} TEMPLATE template0 ENCODING 'UTF-8'")

        with DBConn(self.args.dsn) as conn:
            print(f'Loading schema')
            schema_path = os.path.join(self.schema_dir, 'schema.sql')
            with open(schema_path, 'r') as schema:
                conn.cursor.execute(schema.read())

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
        analyze_tasks = [f'psql {self.args.dsn} -c "ANALYZE {table}"' for table in
                         PrepareBenchmarkFactory.TABLES]
        self._run_tasks_parallel(analyze_tasks)

    def supports_cluster(self):
        result = self._run_shell_task(
                # Check if there is a 'cluster' function in the 'swarm64da' namespace
                f'psql {self.args.dsn} --tuples-only --no-align -c "\dfn swarm64da.cluster"',
                return_output=True)
        return result.startswith(b'swarm64da|cluster')

    def cluster(self):
        cluster_tasks = [
                f'''psql {self.args.dsn} -c "SELECT swarm64da.cluster('{table}', '{colspec}')"'''
                for table, colspec in PrepareBenchmarkFactory.CLUSTER_SPEC.items()]
        self._run_tasks_parallel(cluster_tasks)
