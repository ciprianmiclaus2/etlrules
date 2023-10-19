import datetime
import os
from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

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
            assert_frame_equal(data.get_named_output("result"), TEST_DF)
    finally:
        os.remove(os.path.join("/tmp", "tst.parquet"))
