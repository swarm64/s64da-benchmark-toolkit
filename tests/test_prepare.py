import pytest

from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory
from s64da_benchmark_toolkit.streams import Benchmark


@pytest.fixture()
def prepare_mock(mocker):
    class Args:
        schema = 'sdb_hooray'
        scale_factor = 100
        data_dir = 'some/fancy/dir'
        num_partitions = 1

    class PrepareTestBenchmark(PrepareBenchmarkFactory):
        PrepareBenchmarkFactory.SIZING_FACTORS = {
            's64da': {
                100: 0.5
            }
        }

    mocker.patch('os.path.isdir', return_value=True)
    return PrepareTestBenchmark(Args(),Benchmark('foobar', '/bla/bleh/foobar'))


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
