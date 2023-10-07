import datetime
from pandas import DataFrame, Timestamp
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.backends.pandas import (
    DateTimeLocalNowRule, DateTimeUTCNowRule, DateTimeToStrFormatRule,
    DateTimeRoundRule, DateTimeRoundDownRule, DateTimeRoundUpRule, 
)
from tests.backends.pandas.utils.data import get_test_data


def test_utcnow_rule():
    input_df = DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeUTCNowRule(output_column='TimeNow', named_input="input", named_output="result")
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A", "TimeNow"]
        assert all((x - datetime.datetime.utcnow()).total_seconds() < 5 for x in result["TimeNow"])


def test_utcnow_existing_column_strict():
    input_df = DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeUTCNowRule(output_column='A', named_input="input", named_output="result")
        with pytest.raises(ColumnAlreadyExistsError):
            rule.apply(data)


def test_utcnow_existing_column_non_strict():
    input_df = DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeUTCNowRule(output_column='A', named_input="input", named_output="result", strict=False)
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A"]
        assert all((x - Timestamp.now()).total_seconds() < 5 for x in result["A"])


def test_localnow_rule():
    input_df = DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeLocalNowRule(output_column='TimeNow', named_input="input", named_output="result")
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A", "TimeNow"]
        assert all((x - datetime.datetime.now()).total_seconds() < 5 for x in result["TimeNow"])


def test_localnow_existing_column_strict():
    input_df = DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeLocalNowRule(output_column='A', named_input="input", named_output="result")
        with pytest.raises(ColumnAlreadyExistsError):
            rule.apply(data)


def test_localnow_existing_column_non_strict():
    input_df = DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeLocalNowRule(output_column='A', named_input="input", named_output="result", strict=False)
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A"]
        assert all((x - Timestamp.now()).total_seconds() < 5 for x in result["A"])


@pytest.mark.parametrize("columns,format,output_columns,input_df,expected", [
    [["A"], "%Y-%m-%d %H:%M:%S", None, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), DataFrame(data=[
        {"A": "2023-05-15 09:15:45"},
        {"A": "2023-05-16 19:25:00"},
    ])],
    [["A", "B"], "%Y-%m-%d %H:%M", None, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), DataFrame(data=[
        {"A": "2023-05-15 09:15", "B": "2023-07-15 09:15"},
        {"A": "2023-05-16 19:25"},
    ])],
    [["A", "B"], "%Y-%m-%d %H:%M", ["E", "F"], DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45), "E": "2023-05-15 09:15", "F": "2023-07-15 09:15"},
        {"A": datetime.datetime(2023, 5, 16, 19, 25), "E": "2023-05-16 19:25"},
    ])],
    [["A", "Z"], "%Y-%m-%d %H:%M", ["E", "F"], DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), MissingColumnError],
    [["A", "B"], "%Y-%m-%d %H:%M", ["E", "A"], DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), ColumnAlreadyExistsError],
])
def test_str_format(columns, format, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeToStrFormatRule(
            columns, format=format,
            output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_DF = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
    {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
])

@pytest.mark.parametrize("rule_cls,columns,granularity,output_columns,input_df,expected", [
    [DateTimeRoundRule, ["A", "B"], "day", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15), "B": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 15), "B": datetime.datetime(2023, 7, 16)},
    ])],
    [DateTimeRoundRule, ["A", "B"], "day", ["E", "F"], INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999), "E": datetime.datetime(2023, 5, 15), "F": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25), "E": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1), "E": datetime.datetime(2023, 5, 15), "F": datetime.datetime(2023, 7, 16)},
    ])],
    [DateTimeRoundRule, ["A", "B"], "hour", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9), "B": datetime.datetime(2023, 7, 15, 10)},
        {"A": datetime.datetime(2023, 5, 16, 19)},
        {"A": datetime.datetime(2023, 5, 15, 12), "B": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundRule, ["A", "B"], "minute", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 16), "B": datetime.datetime(2023, 7, 15, 9, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0), "B": datetime.datetime(2023, 7, 15, 12, 0)},
    ])],
    [DateTimeRoundRule, ["A", "B"], "second", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 45, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ])],
    [DateTimeRoundRule, ["A", "B"], "millisecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 10000), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 100000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ])],
    [DateTimeRoundRule, ["A", "B"], "microsecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],
    [DateTimeRoundRule, ["A", "B"], "nanosecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],

    [DateTimeRoundDownRule, ["A", "B"], "day", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15), "B": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 16)},
        {"A": datetime.datetime(2023, 5, 15), "B": datetime.datetime(2023, 7, 15)},
    ])],
    [DateTimeRoundDownRule, ["A", "B"], "hour", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9), "B": datetime.datetime(2023, 7, 15, 9)},
        {"A": datetime.datetime(2023, 5, 16, 19)},
        {"A": datetime.datetime(2023, 5, 15, 12), "B": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundDownRule, ["A", "B"], "minute", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15), "B": datetime.datetime(2023, 7, 15, 9, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12), "B": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundDownRule, ["A", "B"], "second", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 45, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12), "B": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundDownRule, ["A", "B"], "millisecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9000), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ])],
    [DateTimeRoundDownRule, ["A", "B"], "microsecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],
    [DateTimeRoundDownRule, ["A", "B"], "nanosecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],

    [DateTimeRoundUpRule, ["A", "B"], "day", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 16), "B": datetime.datetime(2023, 7, 16)},
        {"A": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 16), "B": datetime.datetime(2023, 7, 16)},
    ])],
    [DateTimeRoundUpRule, ["A", "B"], "hour", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 10), "B": datetime.datetime(2023, 7, 15, 10)},
        {"A": datetime.datetime(2023, 5, 16, 20)},
        {"A": datetime.datetime(2023, 5, 15, 12), "B": datetime.datetime(2023, 7, 15, 13)},
    ])],
    [DateTimeRoundUpRule, ["A", "B"], "minute", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 16), "B": datetime.datetime(2023, 7, 15, 9, 46)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12), "B": datetime.datetime(2023, 7, 15, 12, 1)},
    ])],
    [DateTimeRoundUpRule, ["A", "B"], "second", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 46), "B": datetime.datetime(2023, 7, 15, 9, 45, 16)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 1)},
    ])],
    [DateTimeRoundUpRule, ["A", "B"], "millisecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 10000), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 100000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1000)},
    ])],
    [DateTimeRoundUpRule, ["A", "B"], "microsecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],
    [DateTimeRoundUpRule, ["A", "B"], "nanosecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],

    [DateTimeRoundRule, ["A", "Z"], "day", None, INPUT_DF, MissingColumnError],
    [DateTimeRoundRule, ["A", "B"], "day", ["B", "A"], INPUT_DF, ColumnAlreadyExistsError],
    [DateTimeRoundDownRule, ["A", "Z"], "day", None, INPUT_DF, MissingColumnError],
    [DateTimeRoundDownRule, ["A", "B"], "day", ["B", "A"], INPUT_DF, ColumnAlreadyExistsError],
    [DateTimeRoundUpRule, ["A", "Z"], "day", None, INPUT_DF, MissingColumnError],
    [DateTimeRoundUpRule, ["A", "B"], "day", ["B", "A"], INPUT_DF, ColumnAlreadyExistsError],
])
def test_round_trunc_rules(rule_cls, columns, granularity, output_columns, input_df, expected):
    a = [DateTimeRoundUpRule, ["A", "B"], "day", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "B": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "B": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])]
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(
            columns, granularity,
            output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False