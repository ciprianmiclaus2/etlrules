import datetime
import glob
import os
from pandas import DataFrame
import pytest

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas import ReadParquetFileRule, WriteParquetFileRule
from etlrules.backends.common.dataframe import select_columns
from tests.utils.data import assert_frame_equal, get_test_data


TEST_DF = [
    {"A": 1, "B": True, "C": "c1", "D": datetime.datetime(2023, 5, 23, 10, 30, 45)},
    {"A": 2, "B": False, "C": "c2", "D": datetime.datetime(2023, 5, 24, 11, 30, 45)},
    {"A": 3, "B": True, "C": "c3", "D": datetime.datetime(2023, 5, 25, 12, 30, 45)},
    {"B": False, "D": datetime.datetime(2023, 5, 26, 13, 30, 45)},
    {"A": 4, "C": "c4"},
    {}
]


RESULT3_DF = [
    {"A": 3, "C": "c3"},
    {"A": 4, "C": "c4"},
]

RESULT4_DF = [
    {"A": 3, "C": "c3"},
]

RESULT5_DF = [
    {"A": 1, "C": "c1"},
    {"A": 3, "C": "c3"},
]


@pytest.mark.parametrize("compression",
    [None] + 
    list(WriteParquetFileRule.COMPRESSIONS)
)
def test_write_read_parquet_file(compression, backend):
    try:
        test_df = backend.DataFrame(data=TEST_DF, astype={"A": "Int64", "B": "boolean"})
        result3_df = backend.DataFrame(RESULT3_DF, astype={"A": "Int64"})
        result4_df = backend.DataFrame(RESULT4_DF, astype={"A": "Int64"})
        result5_df = backend.DataFrame(RESULT5_DF, astype={"A": "Int64"})
        with get_test_data(test_df, named_inputs={"input": test_df}, named_output="result") as data:
            write_rule = backend.rules.WriteParquetFileRule(file_name="tst.parquet", file_dir="/tmp", compression=compression, named_input="input")
            write_rule.apply(data)
            read_rule = backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", named_output="result")
            read_rule.apply(data)
            read_rule = backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], named_output="result2")
            read_rule.apply(data)
            read_rule = backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], filters=[("A", ">=", 3)], named_output="result3")
            read_rule.apply(data)
            read_rule = backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], filters=[("A", ">=", 3), ("B", "==", True)], named_output="result4")
            read_rule.apply(data)
            read_rule = backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], filters=[[("A", ">=", 3), ("B", "==", True)], [("C", "in", ("c1", "c3"))]], named_output="result5")
            read_rule.apply(data)
            actual = data.get_named_output("result")
            assert_frame_equal(actual, test_df)
            assert_frame_equal(data.get_named_output("result2"), select_columns(test_df, ["A", "C"]))
            assert_frame_equal(data.get_named_output("result3"), result3_df)
            assert_frame_equal(data.get_named_output("result4"), result4_df)
            assert_frame_equal(data.get_named_output("result5"), result5_df)
    finally:
        for f in glob.glob(os.path.join("/tmp", "tst*.parquet")):
            os.remove(f)



@pytest.mark.parametrize("filters", [
    "invalid", [1, 2, 3], ["col", "==", 1], [("col", "==")], [("col", "==", 1, 2)],
    [[("col", "==", 1, 5)]], [[("col", "==", 1), ("col", ">", 2)], [("col", "<=")]],
    [("col", "!==", 1)], [("col", "in", 3)],
    ]
)
def test_invalid_filters(filters, backend):
    with pytest.raises(ValueError):
        backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", filters=filters, named_output="result")


def test_write_read_parquet_file_invalid_columns(backend):
    try:
        test_df = backend.DataFrame(data=TEST_DF, astype={"A": "Int64", "B": "boolean"})
        with get_test_data(test_df, named_inputs={"input": test_df}, named_output="result") as data:
            write_rule = backend.rules.WriteParquetFileRule(file_name="tst.parquet", file_dir="/tmp", named_input="input")
            write_rule.apply(data)
            read_rule = backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "M"], named_output="result")
            with pytest.raises(MissingColumnError):
                read_rule.apply(data)
                actual = data.get_named_output("result")
                assert_frame_equal(actual, test_df)
    finally:
        for f in glob.glob(os.path.join("/tmp", "tst*.parquet")):
            os.remove(f)


def test_write_read_parquet_file_invalid_column_in_filters(backend):
    try:
        test_df = backend.DataFrame(data=TEST_DF, astype={"A": "Int64", "B": "boolean"})
        with get_test_data(test_df, named_inputs={"input": test_df}, named_output="result") as data:
            write_rule = backend.rules.WriteParquetFileRule(file_name="tst.parquet", file_dir="/tmp", named_input="input")
            write_rule.apply(data)
            read_rule = backend.rules.ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", filters=[("M", "==", 1)], named_output="result")
            with pytest.raises(MissingColumnError):
                read_rule.apply(data)
    finally:
        for f in glob.glob(os.path.join("/tmp", "tst*.parquet")):
            os.remove(f)


def test_read_parquet_multiple_files(backend, tmp_path):
    # files written by the backend, read back with regex=True and concatenated in the order of their names
    for idx, values in enumerate(([1, 2], [3])):
        df = backend.DataFrame({"A": values}, astype={"A": "Int64"})
        with get_test_data(df, named_inputs={"input": df}, named_output="result") as data:
            backend.rules.WriteParquetFileRule(f"part{idx}.parquet", str(tmp_path), named_input="input").apply(data)
    # dask writes one file per partition: part0_part_0.parquet (without a double dot before the extension)
    assert all(".." not in name for name in os.listdir(tmp_path))
    expected = backend.DataFrame({"A": [1, 2, 3]}, astype={"A": "Int64"})
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        backend.rules.ReadParquetFileRule(r"part\d.*\.parquet", str(tmp_path), regex=True, named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_read_files_regex_no_match(backend, tmp_path):
    # no matching files: an error (it used to produce None, which failed in the next rule)
    (tmp_path / "other.csv").write_text("A\n1\n")
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        for rule in (
            backend.rules.ReadCSVFileRule(r"data_\d+\.csv", str(tmp_path), regex=True, named_output="result"),
            backend.rules.ReadParquetFileRule(r"data_\d+\.parquet", str(tmp_path), regex=True, named_output="result"),
            backend.rules.ReadJSONFileRule(r"data_\d+\.json", str(tmp_path), regex=True, named_output="result"),
        ):
            with pytest.raises(FileNotFoundError) as exc:
                rule.apply(data)
            assert "No files matching" in str(exc.value)
