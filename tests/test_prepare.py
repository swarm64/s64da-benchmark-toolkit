import pytest

from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory
from s64da_benchmark_toolkit.streams import Benchmark
from benchmarks.tpch import prepare as prepare_tpch
from benchmarks.tpcds import prepare as prepare_tpcds


class ArgsMock:
    dsn = 'postgresql://u@h:0/db'
    schema = 'sdb_hooray'
    scale_factor = 100
    data_dir = '/some/fancy/dir'
    num_partitions = None


@pytest.fixture()
def prepare_mock(mocker):
    class PrepareTestBenchmark(PrepareBenchmarkFactory):
        PrepareBenchmarkFactory.SIZING_FACTORS = {
            's64da': {
                100: 0.5
            }
        }

    mocker.patch('os.path.isdir', return_value=True)
    return PrepareTestBenchmark(ArgsMock(), Benchmark('foobar', '/bla/bleh/foobar'))


def test_check_diskspace_ok(mocker, prepare_mock):
    mocker.patch('shutil.disk_usage',
                 return_value=(150 << 30, 50 << 30, 100 << 30),
                 autospec=True)

    # Must not raise an AssertionError
    prepare_mock._check_diskspace('/xyz')


def test_check_diskspace_not_ok(mocker, prepare_mock):
    mocker.patch('shutil.disk_usage',
                 return_value=(150 << 30, 100 << 30, 50 << 30),
                 autospec=True)

    with pytest.raises(AssertionError):
        prepare_mock._check_diskspace('/xyz')


def test_check_diskspace_scale_factor_off(mocker, prepare_mock):
    mocker.patch('shutil.disk_usage',
                 return_value=(150 * 1024 * 1024, 100 * 1024 * 1024, 50 * 1024 * 1024),
                 autospec=True)

    prepare_mock.args.scale_factor = 123

    # Must not raise an AssertionError
    prepare_mock._check_diskspace('/xyz')


@pytest.mark.parametrize('prepare_mod', [prepare_tpch, prepare_tpcds])
def test_get_copy_cmds_returns_array_if_files_exist(mocker, prepare_mod):
    mocker.patch('os.path.isdir', return_value=True)
    mocker.patch.object(prepare_mod, 'glob', return_value=['table.01.gz', 'table.02.gz'])
    args = ArgsMock()
    bm = prepare_mod.PrepareBenchmark(
        args, Benchmark('foobar', '/bla/bleh/foobar')
    )
    assert 2 == len(bm.get_copy_cmds(args.data_dir, 'table'))


@pytest.mark.parametrize('prepare_mod', [prepare_tpch, prepare_tpcds])
@pytest.mark.parametrize('glob_returns', [None, []])
def test_get_copy_cmds_raises_filenotfound_err(mocker, prepare_mod, glob_returns):
    mocker.patch('os.path.isdir', return_value=True)
    mocker.patch.object(prepare_mod, 'glob', return_value=glob_returns)
    args = ArgsMock()
    bm = prepare_mod.PrepareBenchmark(
        args, Benchmark('foobar', '/bla/bleh/foobar')
    )
    with pytest.raises(FileNotFoundError):
        bm.get_copy_cmds(args.data_dir, 'table')
