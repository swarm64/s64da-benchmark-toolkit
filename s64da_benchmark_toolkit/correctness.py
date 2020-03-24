
import logging
import os

import numpy as np
import pandas as pd

from natsort import index_natsorted, order_by_index
from pandas.io.formats.style import Styler

LOG = logging.getLogger()

class Correctness:
    def __init__(self, scale_factor, benchmark):
        self.scale_factor = scale_factor
        self.query_output_folder = os.path.join('results', 'query_results')
        self.correctness_results_folder = os.path.join('correctness_results',
                                                       benchmark, f'sf{self.scale_factor}')

        self.html = ''
        self.diff = None

    def get_correctness_filepath(self, query_id):
        filepath = os.path.join(self.correctness_results_folder, f'{query_id}.csv')
        return filepath

    @classmethod
    def round_to_precision(cls, value):
        return '%.12g' % float('%.2f' % value)

    @classmethod
    def check_for_mismatches(cls, truth, result):
        merge = truth.merge(result, indicator=True, how='left')
        differences = merge.loc[lambda x: x['_merge'] != 'both']

        mismatches = []
        for index, _ in differences.iterrows():
            truth_row = truth.iloc[index]
            result_row = result.iloc[index]

            for column_name, truth_datum in truth_row.iteritems():
                result_datum = result_row[column_name]

                if truth.dtypes[column_name] == 'float64':
                    if np.isnan(truth_datum):
                        matches = (np.isnan(result_datum) == True)
                    elif np.isinf(truth_datum):
                        matches = (np.isinf(result_datum) == True)
                    else:
                        matches = (
                            cls.round_to_precision(truth_datum) == cls.round_to_precision(result_datum))

                elif truth.dtypes[column_name] == 'object':
                    matches = (str(truth_datum) == str(result_datum))
                else:
                    matches = (truth_datum == result_datum)

                if not matches:
                    mismatches.append(index)
                    break

        return mismatches

    def _check_correctness_impl(self, truth, result):
        def prepare(df):
            # Sort columns
            df = df.sort_index(axis=1)
            # Natsort all rows
            df = df.reindex(index=order_by_index(df.index, index_natsorted(zip(df.to_numpy()))))
            # Recreate index for comparison later
            return df

        if truth.empty != result.empty:
            return list(result.index) if truth.empty else list(truth.index)

        if truth.shape != result.shape:
            return list(truth.index)

        truth = prepare(truth)
        result = prepare(result)

        # Column names must be same
        if not truth.columns.difference(result.columns).empty:
            return list(truth.index)

        mismatch_idx = Correctness.check_for_mismatches(truth, result)
        return mismatch_idx

    def check_correctness(self, stream_id, query_number):
        LOG.debug(f'Checking Stream={stream_id}, Query={query_number}')
        correctness_path = self.get_correctness_filepath(query_number)
        benchmark_path = os.path.join(self.query_output_folder, f'{stream_id}_{query_number}.csv')

        # Reading truth
        try:
            truth = pd.read_csv(correctness_path)
        except pd.errors.EmptyDataError:
            LOG.debug(f'Query {query_number} is empty in correctness results.')
            truth = pd.DataFrame(columns=['col'])
        except FileNotFoundError:
            LOG.debug(f'Correctness results for {query_number} not found. Skipping correctness checking.')
            return 'OK'

        # Reading Benchmark results
        try:
            result = pd.read_csv(benchmark_path)
        except pd.errors.EmptyDataError:
            LOG.debug(f'{stream_id}_{query_number}.csv empty in benchmark results.')
            result = pd.DataFrame(columns=['col'])
        except FileNotFoundError:
            msg = f'Query results for {stream_id}-{query_number} not found. Reporting as mismatch.'
            LOG.debug(msg)
            self.html += f'<p>{msg}</p>'
            return 'Mismatch'

        mismatch_idx = self._check_correctness_impl(truth, result)
        if mismatch_idx:
            self.html += Correctness.to_html(
                truth.iloc[mismatch_idx],
                table_title=f'Truth for StreamId={stream_id}, Query={query_number}')

            self.html += Correctness.to_html(
                result.iloc[mismatch_idx],
                table_title=f'Result for StreamId={stream_id}, Query={query_number}')

            return 'Mismatch'

        return 'OK'

    @staticmethod
    def to_html(df, table_title):
        Swarm64Styler = Styler.from_custom_template("resources", "report.tpl")

        return Swarm64Styler(df).render(table_title=table_title)
