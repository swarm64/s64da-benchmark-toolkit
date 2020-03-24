import pytest
import pandas as pd

from s64da_benchmark_toolkit import correctness


@pytest.fixture()
def cc():
    benchmark = 'tpch'
    scale_factor = 1000

    return correctness.Correctness(scale_factor, benchmark)


@pytest.fixture()
def monkeypatch_read_csv(monkeypatch):
    def read_csv(filepath, *args, **kwargs):
        if filepath == 'foobar':
            return pd.DataFrame({'foo': ['Hello'], 'bar': ['World']})
        elif filepath == 'foo':
            return pd.DataFrame({'foobar': ['Hello']})
        elif filepath == 'bar':
            return pd.DataFrame({'foobar': ['World']})
        else:
            return pd.DataFrame(columns=['col'])

    monkeypatch.setattr(pd, 'read_csv', read_csv)


@pytest.fixture()
def monkeypatch_to_html(monkeypatch):
    def no_to_html(*args, **kwargs):
        return ''

    monkeypatch.setattr(correctness.Correctness, 'to_html', no_to_html)


def test_correctness_filepath(cc):

    filepath = cc.get_correctness_filepath('1')
    assert filepath == f'{cc.correctness_results_folder}/1.csv'


def check_for_mismatches(cc):
    first_df = pd.DataFrame(columns=['col'])
    second_df = pd.DataFrame(columns=['col'])
    foobar = {'foo': ['Hello'], 'bar': ['World']}

    assert not cc.check_for_mismatches(first_df, second_df)
    assert cc.check_for_mismatches(first_df, pd.DataFrame(foobar))
    assert cc.check_for_mismatches(pd.DataFrame(foobar), second_df)
    assert not cc.check_for_mismatches(pd.DataFrame(foobar), pd.DataFrame(foobar))
    assert cc.check_for_mismatches(pd.DataFrame({'foobar': ['Hello']}),
                              pd.DataFrame({'foobar': ['World']}))


def test_check_correctness_no_correctness_file(cc, mocker):

    mocked_filepath = mocker.patch('s64da_benchmark_toolkit.correctness.Correctness.get_correctness_filepath')
    mocked_filepath.return_value='/nopath/nofile.csv'

    assert cc.check_correctness(1, 1) == 'OK'


def test_check_correctness_mismatch(cc, monkeypatch_read_csv, monkeypatch_to_html, mocker):

    mocked_filepath = mocker.patch('s64da_benchmark_toolkit.correctness.Correctness.get_correctness_filepath')
    mocked_filepath.return_value = 'foo'

    mocked_os_join = mocker.patch('s64da_benchmark_toolkit.correctness.os.path.join')
    mocked_os_join.return_value = 'bar'

    assert cc.check_correctness(1, 1) == 'Mismatch'


def test_check_correctness_ok(cc, monkeypatch_read_csv, monkeypatch_to_html, mocker):

    mocked_filepath = mocker.patch('s64da_benchmark_toolkit.correctness.Correctness.get_correctness_filepath')
    mocked_filepath.return_value = 'foobar'

    mocked_os_join = mocker.patch('s64da_benchmark_toolkit.correctness.os.path.join')
    mocked_os_join.return_value = 'foobar'

    assert cc.check_correctness(1, 1) == 'OK'


def test_check_correctness_empty(cc, monkeypatch_read_csv, monkeypatch_to_html, mocker):

    mocked_filepath = mocker.patch('s64da_benchmark_toolkit.correctness.Correctness.get_correctness_filepath')
    mocked_filepath.return_value = 'empty'

    mocked_os_join = mocker.patch('s64da_benchmark_toolkit.correctness.os.path.join')
    mocked_os_join.return_value = 'empty'

    assert cc.check_correctness(1, 1) == 'OK'
