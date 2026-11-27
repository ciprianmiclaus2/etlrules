import bz2
import datetime
import gzip
import json
import lzma
import zipfile

import pytest

from etlrules.backends.common.io.files import WriteJSONFileRule
from etlrules.exceptions import MissingColumnError, UnsupportedTypeError
from tests.utils.data import assert_frame_equal, get_test_data


TEST_DF = [
    {"I": 1, "F": 1.5, "S": "a", "B": True},
    {"I": 2, "S": "b é", "B": False},
    {"F": 2.0, "S": "c"},
    {},
]
TEST_TYPES = {"I": "Int64", "F": "Float64", "S": "string", "B": "boolean"}

STDLIB_OPENERS = {
    None: lambda path: open(path, "rt", encoding="utf-8"),
    "gzip": lambda path: gzip.open(path, "rt", encoding="utf-8"),
    "bz2": lambda path: bz2.open(path, "rt", encoding="utf-8"),
    "xz": lambda path: lzma.open(path, "rt", encoding="utf-8"),
}


def read_lines(path, compression):
    """ Reads the JSON lines file with python's standard library. """
    if compression == "zip":
        with zipfile.ZipFile(path) as archive:
            assert len(archive.namelist()) == 1
            contents = archive.read(archive.namelist()[0]).decode("utf-8")
    else:
        with STDLIB_OPENERS[compression](path) as f:
            contents = f.read()
    return [json.loads(line) for line in contents.splitlines() if line.strip()]


@pytest.mark.parametrize("compression", [None] + list(WriteJSONFileRule.COMPRESSIONS))
def test_write_read_json_file(compression, backend, tmp_path):
    file_name = "tst.json" + (WriteJSONFileRule.COMPRESSIONS[compression] if compression else "")
    test_df = backend.DataFrame(TEST_DF, astype=TEST_TYPES)
    with get_test_data(test_df, named_inputs={"input": test_df}, named_output="result") as data:
        backend.rules.WriteJSONFileRule(file_name, str(tmp_path), compression=compression, named_input="input").apply(data)
        # the file is valid JSON lines, readable by other tools
        rows = read_lines(tmp_path / file_name, compression)
        assert len(rows) == len(TEST_DF)
        assert rows[0] == {"I": 1, "F": 1.5, "S": "a", "B": True}
        assert rows[1]["S"] == "b é" and rows[1]["F"] is None
        # read back with the same (nullable) types in all the backends
        backend.rules.ReadJSONFileRule(file_name, str(tmp_path), named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), test_df)


def test_write_json_datetimes(backend, tmp_path):
    # the same datetime format in all the backends
    test_df = backend.DataFrame([
        {"A": 1, "T": datetime.datetime(2023, 5, 19, 10, 30, 45)},
        {"A": 2, "T": datetime.datetime(2024, 2, 29, 23, 59, 59, 123456)},
        {"A": 3},
    ], astype={"A": "Int64", "T": "datetime"})
    with get_test_data(test_df, named_inputs={"input": test_df}, named_output="result") as data:
        backend.rules.WriteJSONFileRule("dt.json", str(tmp_path), named_input="input").apply(data)
        assert [row.get("T") for row in read_lines(tmp_path / "dt.json", None)] == [
            "2023-05-19T10:30:45.000000", "2024-02-29T23:59:59.123456", None]
        # read back as strings, which StrToDateTimeRule converts back to datetimes
        backend.rules.ReadJSONFileRule("dt.json", str(tmp_path), named_output="strings").apply(data)
        backend.rules.StrToDateTimeRule("T", "%Y-%m-%dT%H:%M:%S.%f", named_input="strings", named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), test_df)


def test_read_json_date_strings_stay_strings(backend, tmp_path):
    # pandas converts columns named like dates and pyarrow converts strings that look like dates: they're read as strings
    (tmp_path / "d.json").write_text(
        '{"created_at": "2023-05-19 10:00:00", "D": "2023-05-19", "date": "2023-05-19T10:00:00"}\n'
        '{"created_at": "not a date", "D": "2023-05-20", "date": null}\n')
    expected = backend.DataFrame([
        {"created_at": "2023-05-19 10:00:00", "D": "2023-05-19", "date": "2023-05-19T10:00:00"},
        {"created_at": "not a date", "D": "2023-05-20"},
    ], astype={"created_at": "string", "D": "string", "date": "string"})
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        backend.rules.ReadJSONFileRule("d.json", str(tmp_path), named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_read_json_column_types(backend, tmp_path):
    (tmp_path / "t.json").write_text('{"A": 1, "B": "x", "C": null}\n{"A": 2, "B": null, "C": null}\n')
    expected = backend.DataFrame({"A": [1, 2], "B": ["x", None], "C": [None, None]}, astype={"A": "Int32", "B": "string", "C": "Float64"})
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        # C only has nulls: its type can't be inferred, column_types sets it
        backend.rules.ReadJSONFileRule("t.json", str(tmp_path), column_types={"A": "int32", "B": "string", "C": "float64"}, named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_read_json_column_types_missing_column(backend, tmp_path):
    (tmp_path / "t.json").write_text('{"A": 1}\n')
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        with pytest.raises(MissingColumnError):
            backend.rules.ReadJSONFileRule("t.json", str(tmp_path), column_types={"Z": "int64"}, named_output="result").apply(data)
        # non strict: ignored
        backend.rules.ReadJSONFileRule("t.json", str(tmp_path), column_types={"Z": "int64"}, named_output="result", strict=False).apply(data)
    with pytest.raises(UnsupportedTypeError):
        backend.rules.ReadJSONFileRule("t.json", str(tmp_path), column_types={"A": "unknown"})


def test_read_json_multiple_files(backend, tmp_path):
    # the files are concatenated in the order of their names; missing columns are filled with missing values
    (tmp_path / "data_2.json").write_text('{"A": 3, "B": "c", "C": 1.5}\n')
    (tmp_path / "data_1.json").write_text('{"A": 1, "B": "a"}\n{"A": 2, "B": "b"}\n')
    (tmp_path / "other.json").write_text('{"A": 100}\n')
    expected = backend.DataFrame([{"A": 1, "B": "a"}, {"A": 2, "B": "b"}, {"A": 3, "B": "c", "C": 1.5}],
                                 astype={"A": "Int64", "B": "string", "C": "Float64"})
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        backend.rules.ReadJSONFileRule(r"data_\d\.json", str(tmp_path), regex=True, named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_read_csv_multiple_files(backend, tmp_path):
    # reading multiple files with regex=True (it used to fail in the pandas, polars and dask backends)
    (tmp_path / "data1.csv").write_text("A,B\n1,a\n")
    (tmp_path / "data2.csv").write_text("A,B\n2,b\n3,c\n")
    expected = backend.DataFrame([{"A": 1, "B": "a"}, {"A": 2, "B": "b"}, {"A": 3, "B": "c"}])
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        backend.rules.ReadCSVFileRule(r"data\d\.csv", str(tmp_path), regex=True, named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_write_json_compression_extension():
    with pytest.raises(AssertionError):
        WriteJSONFileRule("data.json", compression="gzip")
    with pytest.raises(AssertionError):
        WriteJSONFileRule("data.json.gz", compression="unknown")
