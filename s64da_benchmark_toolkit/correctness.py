import logging
import os

from enum import Enum

import numpy as np
import pandas as pd

from natsort import index_natsorted, order_by_index

LOG = logging.getLogger()


class CorrectnessResult:
    def __init__(self, status, detail=None, truth=[], result=[]):
        self.status = status
        self.detail = detail
        self.truth = truth
        self.result = result

    @classmethod
    def make_mismatch_result(cls, detail, truth, result):
        return cls("MISMATCH", detail=detail, truth=truth, result=result)

    @classmethod
    def make_ok_result(cls):
        return cls("OK")

    @property
    def is_ok(self):
        return self.status == "OK"

    @property
    def is_mismatch(self):
        return self.status == "MISMATCH"

    def to_html(self):
        status = self.status
        if self.is_ok:
            return status

        def check_for_empty_df_then_convert_to_html(df):
            if isinstance(df, list) and df == []:
                return "None"
            else:
                return df.to_html()

        # HTML here, since it'll be used for reporting to HTML
        truth_html = check_for_empty_df_then_convert_to_html(self.truth)
        result_html = check_for_empty_df_then_convert_to_html(self.result)
        return f"{status}<br /><div>{truth_html}</div><br /><div>{result_html}</div>"

    def __repr__(self):
        return self.status


class ResultDetail(Enum):
    OK = 1
    TRUTH_EMPTY = 2
    RESULT_EMPTY = 3
    SHAPE_MISMATCH = 4
    COLUMNS_MISMATCH = 5
    VALUE_MISMATCH = 6


class Correctness:
    def __init__(self, scale_factor, benchmark):
        self.scale_factor = scale_factor
        self.query_output_folder = os.path.join("results", "query_results")
        self.correctness_results_folder = os.path.join(
            "correctness_results", benchmark, f"sf{self.scale_factor}"
        )

    def get_correctness_filepath(self, query_id):
        filepath = os.path.join(self.correctness_results_folder, f"{query_id}.csv")
        return filepath

    @classmethod
    def round_to_precision(cls, value):
        rounded = "%.2f" % value
        if "." in rounded:
            return rounded[0:13]
        else:
            return rounded[0:12]

    @classmethod
    def match_double_precision(cls, truth_value, result_value):
        truth_rounded = cls.round_to_precision(truth_value)
        result_rounded = cls.round_to_precision(result_value)
        return (
            truth_rounded == result_rounded or abs(truth_value - result_value) <= 0.01
        )

    def prepare(self, df):
        # Sort columns
        df = df.sort_index(axis=1)
        # Natsort all rows
        df = df.reindex(
            index=order_by_index(df.index, index_natsorted(zip(df.to_numpy())))
        )
        # Recreate index for comparison later
        df.reset_index(level=0, drop=True, inplace=True)
        return df

    @classmethod
    def check_for_mismatches(cls, truth, result):
        merge = truth.merge(result, indicator=True, how="left")
        differences = merge.loc[lambda x: x["_merge"] != "both"]

        mismatches = []
        for index, _ in differences.iterrows():
            truth_row = truth.iloc[index]
            result_row = result.iloc[index]

            for column_name, truth_datum in truth_row.iteritems():
                result_datum = result_row[column_name]

                if truth.dtypes[column_name] == "float64":
                    if np.isnan(truth_datum):
                        matches = np.isnan(result_datum) == True
                    elif np.isinf(truth_datum):
                        matches = np.isinf(result_datum) == True
                    else:
                        matches = cls.match_double_precision(truth_datum, result_datum)

                elif truth.dtypes[column_name] == "object":
                    matches = str(truth_datum) == str(result_datum)
                else:
                    matches = truth_datum == result_datum

                if not matches:
                    mismatches.append(index)
                    break

        return mismatches

    def _check_correctness_impl(self, truth, result):

        if truth.empty != result.empty:
            return (
                (ResultDetail.TRUTH_EMPTY, None)
                if truth.empty
                else (ResultDetail.RESULT_EMPTY, None)
            )

        if truth.shape != result.shape:
            return (ResultDetail.SHAPE_MISMATCH, None)

        truth.drop_duplicates(inplace=True, ignore_index=True)
        result.drop_duplicates(inplace=True, ignore_index=True)

        if truth.shape != result.shape:
            LOG.debug("Rows mismatch after dropping duplicates")
            return (ResultDetail.SHAPE_MISMATCH, None)

        truth = self.prepare(truth)
        result = self.prepare(result)

        # Column names must be same
        if not truth.columns.difference(result.columns).empty:
            return (ResultDetail.COLUMNS_MISMATCH, None)

        mismatch_indices = Correctness.check_for_mismatches(truth, result)
        if mismatch_indices:
            return (ResultDetail.VALUE_MISMATCH, mismatch_indices)

        return (ResultDetail.OK, None)

    def check_correctness(self, stream_id, query_number):
        LOG.debug(f"Checking Stream={stream_id}, Query={query_number}")
        correctness_path = self.get_correctness_filepath(query_number)
        benchmark_path = os.path.join(
            self.query_output_folder, f"{stream_id}_{query_number}.csv"
        )

        # Reading truth
        try:
            truth = pd.read_csv(correctness_path)
        except pd.errors.EmptyDataError:
            LOG.debug(f"Query {query_number} is empty in correctness results.")
            truth = pd.DataFrame(columns=["col"])
        except FileNotFoundError:
            LOG.debug(
                f"Correctness results for {query_number} not found. Skipping correctness checking."
            )
            return CorrectnessResult.make_ok_result()

        # Reading Benchmark results
        try:
            result = pd.read_csv(benchmark_path, float_precision="round_trip")
        except pd.errors.EmptyDataError:
            LOG.debug(f"{stream_id}_{query_number}.csv empty in benchmark results.")
            result = pd.DataFrame(columns=["col"])
        except FileNotFoundError:
            msg = f"Query results for {stream_id}-{query_number} not found. Reporting as mismatch."
            LOG.debug(msg)
            return CorrectnessResult.make_mismatch_result(
                ResultDetail.RESULT_EMPTY, [], []
            )

        result_detail, mismatch_indexes = self._check_correctness_impl(truth, result)
        if result_detail == ResultDetail.OK:
            return CorrectnessResult.make_ok_result()

        elif result_detail == ResultDetail.VALUE_MISMATCH:
            truth = self.prepare(truth)
            result = self.prepare(result)
            return CorrectnessResult.make_mismatch_result(
                result_detail, truth.loc[mismatch_indexes], result.loc[mismatch_indexes]
            )

        return CorrectnessResult.make_mismatch_result(result_detail, [], [])
