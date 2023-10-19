import datetime
import os
from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.backends.pandas import ReadCSVFileRule, WriteCSVFileRule
from tests.backends.pandas.utils.data import get_test_data


TEST_DF = DataFrame(data=[
    {"A": 1, "B": True, "C": "c1", "D": datetime.datetime(2023, 5, 23, 10, 30, 45)},
    {"A": 2, "B": False, "C": "c2", "D": datetime.datetime(2023, 5, 24, 11, 30, 45)},
    {"A": 3, "B": True, "C": "c3", "D": datetime.datetime(2023, 5, 25, 12, 30, 45)},
    {"B": False, "D": datetime.datetime(2023, 5, 26, 13, 30, 45)},
    {"A": 4, "C": "c4"},
    {}
])


@pytest.mark.parametrize("compression",
    [None] + 
    list(WriteCSVFileRule.COMPRESSIONS)
)
def test_write_read_csv_file(compression):
    extension = WriteCSVFileRule.COMPRESSIONS[compression] if compression else ""
    try:
        with get_test_data(TEST_DF, named_inputs={"input": TEST_DF}, named_output="result") as data:
            write_rule = WriteCSVFileRule(file_name="tst.csv" + extension, file_dir="/tmp", compression=compression, named_input="input")
            write_rule.apply(data)
            read_rule = ReadCSVFileRule(file_name="tst.csv" + extension, file_dir="/tmp", named_output="result")
            read_rule.apply(data)
            result = data.get_named_output("result")
            result = result.astype({"D": "datetime64[ns]"})
            assert_frame_equal(result, TEST_DF)
    finally:
        os.remove(os.path.join("/tmp", "tst.csv" + extension))


def test_write_read_csv_file_separator():
    try:
        with get_test_data(TEST_DF, named_inputs={"input": TEST_DF}, named_output="result") as data:
            write_rule = WriteCSVFileRule(file_name="tst.csv", file_dir="/tmp", separator="|", named_input="input")
            write_rule.apply(data)
            read_rule = ReadCSVFileRule(file_name="tst.csv", file_dir="/tmp", separator="|", named_output="result")
            read_rule.apply(data)
            result = data.get_named_output("result")
            result = result.astype({"D": "datetime64[ns]"})
            assert_frame_equal(result, TEST_DF)
    finally:
        os.remove(os.path.join("/tmp", "tst.csv"))


def test_write_read_csv_file_no_header():
    try:
        with get_test_data(TEST_DF, named_inputs={"input": TEST_DF}, named_output="result") as data:
            write_rule = WriteCSVFileRule(file_name="tst.csv", file_dir="/tmp", header=False, named_input="input")
            write_rule.apply(data)
            read_rule = ReadCSVFileRule(file_name="tst.csv", file_dir="/tmp", header=False, named_output="result")
            read_rule.apply(data)
            result = data.get_named_output("result")
            result = result.rename(columns={0: "A", 1: "B", 2: "C", 3: "D"})
            result = result.astype({"D": "datetime64[ns]"})
            assert_frame_equal(result, TEST_DF)
    finally:
        os.remove(os.path.join("/tmp", "tst.csv"))