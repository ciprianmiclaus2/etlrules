import datetime
import os
from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas import ReadCSVFileRule, ReadParquetFileRule, WriteCSVFileRule, WriteParquetFileRule
from tests.backends.pandas.utils.data import get_test_data


TEST_DF = DataFrame(data=[
    {"A": 1, "B": True, "C": "c1", "D": datetime.datetime(2023, 5, 23, 10, 30, 45)},
    {"A": 2, "B": False, "C": "c2", "D": datetime.datetime(2023, 5, 24, 11, 30, 45)},
    {"A": 3, "B": True, "C": "c3", "D": datetime.datetime(2023, 5, 25, 12, 30, 45)},
    {"B": False, "D": datetime.datetime(2023, 5, 26, 13, 30, 45)},
    {"A": 4, "C": "c4"},
    {}
])


RESULT3_DF = DataFrame(data=[
    {"A": 3, "C": "c3"},
    {"A": 4, "C": "c4"},
]).astype({"A": "float64"})

RESULT4_DF = DataFrame(data=[
    {"A": 3, "C": "c3"},
]).astype({"A": "float64"})

RESULT5_DF = DataFrame(data=[
    {"A": 1, "C": "c1"},
    {"A": 3, "C": "c3"},
]).astype({"A": "float64"})


@pytest.mark.parametrize("compression",
    [None] + 
    list(WriteParquetFileRule.COMPRESSIONS)
)
def test_write_read_parquet_file(compression):
    try:
        with get_test_data(TEST_DF, named_inputs={"input": TEST_DF}, named_output="result") as data:
            write_rule = WriteParquetFileRule(file_name="tst.parquet", file_dir="/tmp", compression=compression, named_input="input")
            write_rule.apply(data)
            read_rule = ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", named_output="result")
            read_rule.apply(data)
            read_rule = ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], named_output="result2")
            read_rule.apply(data)
            read_rule = ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], filters=[("A", ">=", 3)], named_output="result3")
            read_rule.apply(data)
            read_rule = ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], filters=[("A", ">=", 3), ("B", "==", True)], named_output="result4")
            read_rule.apply(data)
            read_rule = ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "C"], filters=[[("A", ">=", 3), ("B", "==", True)], [("C", "in", ("c1", "c3"))]], named_output="result5")
            read_rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), TEST_DF)
            assert_frame_equal(data.get_named_output("result2"), TEST_DF[["A", "C"]])
            assert_frame_equal(data.get_named_output("result3"), RESULT3_DF)
            assert_frame_equal(data.get_named_output("result4"), RESULT4_DF)
            assert_frame_equal(data.get_named_output("result5"), RESULT5_DF)
    finally:
        os.remove(os.path.join("/tmp", "tst.parquet"))



@pytest.mark.parametrize("filters",
    ["invalid", [1, 2, 3], ["col", "==", 1], [("col", "==")], [("col", "==", 1, 2)],
    [[("col", "==", 1, 5)]], [[("col", "==", 1), ("col", ">", 2)], [("col", "<=")]],
    [("col", "!==", 1)], [("col", "in", 3)],
    ]
)
def test_invalid_filters(filters):
    with pytest.raises(ValueError):
        ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", filters=filters, named_output="result")


def test_write_read_parquet_file_columns():
    try:
        with get_test_data(TEST_DF, named_inputs={"input": TEST_DF}, named_output="result") as data:
            write_rule = WriteParquetFileRule(file_name="tst.parquet", file_dir="/tmp", named_input="input")
            write_rule.apply(data)
            read_rule = ReadParquetFileRule(file_name="tst.parquet", file_dir="/tmp", columns=["A", "M"], named_output="result")
            with pytest.raises(MissingColumnError):
                read_rule.apply(data)
    finally:
        os.remove(os.path.join("/tmp", "tst.parquet"))