import datetime
from pandas import DataFrame, Timestamp
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.backends.pandas import (
    DateTimeLocalNowRule, DateTimeUTCNowRule, DateTimeToStrFormatRule,
    DateTimeRoundRule, DateTimeRoundDownRule, DateTimeRoundUpRule,
    DateTimeExtractComponentRule, DateTimeAddRule, DateTimeSubstractRule,
    DateTimeDiffRule,
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


@pytest.mark.parametrize("input_column,format,output_column,input_df,expected", [
    ["A", "%Y-%m-%d %H:%M:%S", None, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), DataFrame(data=[
        {"A": "2023-05-15 09:15:45"},
        {"A": "2023-05-16 19:25:00"},
    ]).astype({"A": "string"})],
    ["A", "%Y-%m-%d %H:%M:%S", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="string")],
    ["A", "%Y-%m-%d %H:%M", None, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), DataFrame(data=[
        {"A": "2023-05-15 09:15", "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": "2023-05-16 19:25"},
    ]).astype({"A": "string"})],
    ["A", "%Y-%m-%d %H:%M", "E", DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45), "E": "2023-05-15 09:15"},
        {"A": datetime.datetime(2023, 5, 16, 19, 25), "E": "2023-05-16 19:25"},
    ]).astype({"E": "string"})],
    ["Z", "%Y-%m-%d %H:%M", "E", DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), MissingColumnError],
    ["A", "%Y-%m-%d %H:%M", "B", DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ]), ColumnAlreadyExistsError],
])
def test_str_format(input_column, format, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeToStrFormatRule(
            input_column, format=format,
            output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_DF = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
    {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
    {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
    {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1),}
])

@pytest.mark.parametrize("rule_cls, input_column, unit, output_column, input_df, expected", [
    [DateTimeRoundRule, "A", "day", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="datetime64[ns]")],
    [DateTimeRoundDownRule, "A", "day", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="datetime64[ns]")],
    [DateTimeRoundUpRule, "A", "day", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="datetime64[ns]")],
    [DateTimeRoundRule, "A", "day", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 16)},
    ])],
    [DateTimeRoundRule, "A", "day", "E", INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "E": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999), "E": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25), "E": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "E": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1), "E": datetime.datetime(2023, 7, 16)},
    ])],
    [DateTimeRoundRule, "A", "hour", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9)},
        {"A": datetime.datetime(2023, 7, 15, 10)},
        {"A": datetime.datetime(2023, 5, 16, 19)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundRule, "A", "minute", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 16)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0)},
    ])],
    [DateTimeRoundRule, "A", "second", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ])],
    [DateTimeRoundRule, "A", "millisecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 10000)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 100000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ])],
    [DateTimeRoundRule, "A", "microsecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],
    [DateTimeRoundRule, "A", "nanosecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],

    [DateTimeRoundDownRule, "A", "day", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 16)},
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15)},
    ])],
    [DateTimeRoundDownRule, "A", "hour", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9)},
        {"A": datetime.datetime(2023, 7, 15, 9)},
        {"A": datetime.datetime(2023, 5, 16, 19)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundDownRule, "A", "minute", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundDownRule, "A", "second", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ])],
    [DateTimeRoundDownRule, "A", "millisecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9000)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ])],
    [DateTimeRoundDownRule, "A", "microsecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],
    [DateTimeRoundDownRule, "A", "nanosecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],

    [DateTimeRoundUpRule, "A", "day", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 16)},
        {"A": datetime.datetime(2023, 7, 16)},
        {"A": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 16)},
        {"A": datetime.datetime(2023, 7, 16)},
    ])],
    [DateTimeRoundUpRule, "A", "hour", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 10)},
        {"A": datetime.datetime(2023, 7, 15, 10)},
        {"A": datetime.datetime(2023, 5, 16, 20)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 13)},
    ])],
    [DateTimeRoundUpRule, "A", "minute", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 16)},
        {"A": datetime.datetime(2023, 7, 15, 9, 46)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12, 1)},
    ])],
    [DateTimeRoundUpRule, "A", "second", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 46)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 16)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 1)},
    ])],
    [DateTimeRoundUpRule, "A", "millisecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 10000)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 100000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1000)},
    ])],
    [DateTimeRoundUpRule, "A", "microsecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],
    [DateTimeRoundUpRule, "A", "nanosecond", None, INPUT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ])],

    [DateTimeRoundRule, "Z", "day", None, INPUT_DF, MissingColumnError],
    [DateTimeRoundRule, "A", "day", "A", INPUT_DF, ColumnAlreadyExistsError],
    [DateTimeRoundDownRule, "Z", "day", None, INPUT_DF, MissingColumnError],
    [DateTimeRoundDownRule, "A", "day", "A", INPUT_DF, ColumnAlreadyExistsError],
    [DateTimeRoundUpRule, "Z", "day", None, INPUT_DF, MissingColumnError],
    [DateTimeRoundUpRule, "A", "day", "A", INPUT_DF, ColumnAlreadyExistsError],
])
def test_round_trunc_rules(rule_cls, input_column, unit, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(
            input_column, unit, output_column=output_column,
            named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_COMPONENT_DF = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 10, 19, 15, 45, 999)},
    {"A": datetime.datetime(2023, 6, 11, 9, 35, 15, 777)},
    {},
])

INPUT_COMPONENT_DF2 = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 10, 19, 15, 45, 999), "B": 1},
    {"A": datetime.datetime(2023, 6, 11, 9, 35, 15, 777), "B": 2},
    {},
])

@pytest.mark.parametrize("input_column,component,locale,output_column,input_df,expected", [
    ["A", "year", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "year", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 2023},
        {"A": 2023},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "year", None, "E", INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 10, 19, 15, 45, 999), "E": 2023},
        {"A": datetime.datetime(2023, 6, 11, 9, 35, 15, 777), "E": 2023},
        {},
    ]).astype({"E": "Int64"})],
    ["A", "month", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "month", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 5},
        {"A": 6},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "day", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "day", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 10},
        {"A": 11},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "hour", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "hour", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 19},
        {"A": 9},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "minute", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "minute", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 15},
        {"A": 35},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "second", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "second", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 45},
        {"A": 15},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "microsecond", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "microsecond", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 999},
        {"A": 777},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "weekday", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="Int64")],
    ["A", "weekday", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": 2},
        {"A": 6},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "day_name", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="string")],
    ["A", "day_name", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": "Wednesday"},
        {"A": "Sunday"},
        {},
    ]).astype({"A": "string"})],
    ["A", "day_name", "en_US.utf8", None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": "Wednesday"},
        {"A": "Sunday"},
        {},
    ]).astype({"A": "string"})],
    ["A", "month_name", None, None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="string")],
    ["A", "month_name", None, None, INPUT_COMPONENT_DF, DataFrame(data=[
        {"A": "May"},
        {"A": "June"},
        {},
    ]).astype({"A": "string"})],
    ["A", "day_name", "UNKNOWN_LOCALE", None, INPUT_COMPONENT_DF, ValueError],
    ["Z", "year", None, None, INPUT_COMPONENT_DF, MissingColumnError],
    ["A", "year", None, "B", INPUT_COMPONENT_DF2, ColumnAlreadyExistsError],
])
def test_extract_component_rules(input_column, component, locale, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, DataFrame):
            rule = DateTimeExtractComponentRule(
                input_column, component, locale,
                output_column=output_column, named_input="input", named_output="result")
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule = DateTimeExtractComponentRule(
                    input_column, component, locale,
                    output_column=output_column, named_input="input", named_output="result")
                rule.apply(data)
        else:
            assert False


INPUT_ADD_SUB_DF = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100)},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101)},
    {},
])

INPUT_ADD_SUB_DF2 = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "B": 1},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "B": 2},
    {},
])

INPUT_ADD_SUB_DF3 = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "B": datetime.timedelta(days=1)},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "B": datetime.timedelta(days=2)},
    {},
])

INPUT_ADD_SUB_DF4 = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "B": 1},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "B": 2},
    {},
])


@pytest.mark.parametrize("rule_cls, input_column, unit_value, unit, output_column, input_df, expected", [
    [DateTimeAddRule, "A", 40, "days", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="datetime64[ns]")],
    [DateTimeAddRule, "A", 40, "days", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 6, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 7, 20, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", 40, "days", "E", INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "E": datetime.datetime(2023, 6, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "E": datetime.datetime(2023, 7, 20, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", -1, "days", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 9, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 40, "days", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="datetime64[ns]")],
    [DateTimeSubstractRule, "A", -40, "days", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 6, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 7, 20, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 1, "days", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 9, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", 40, "hours", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="datetime64[ns]")],
    [DateTimeAddRule, "A", 40, "hours", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 13, 2, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 12, 3, 21, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 40, "hours", None, DataFrame(data={"A": []}, dtype="datetime64[ns]"), DataFrame(data={"A": []}, dtype="datetime64[ns]")],
    [DateTimeSubstractRule, "A", 40, "hours", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 9, 18, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 8, 19, 21, 31, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", 10, "minutes", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 11, 10, 30, 30, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 31, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 10, "minutes", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 11, 10, 10, 30, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 11, 31, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", 10, "seconds", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 40, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 41, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 10, "seconds", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 20, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 21, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", 10, "microseconds", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 110)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 111)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 10, "microseconds", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 90)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 91)},
        {},
    ])],

    [DateTimeSubstractRule, "A", "B", None, None, DataFrame(data={"A": [], "B": []}, dtype="datetime64[ns]"), DataFrame(data={"A": [], "B": []}).astype({"A": "timedelta64[ns]", "B": "datetime64[ns]"})],

    [DateTimeAddRule, "A", "B", None, None, INPUT_ADD_SUB_DF3, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 12, 10, 20, 30, 100), "B": datetime.timedelta(days=1)},
        {"A": datetime.datetime(2023, 6, 12, 11, 21, 31, 101), "B": datetime.timedelta(days=2)},
        {},
    ])],
    [DateTimeSubstractRule, "A", "B", None, None, INPUT_ADD_SUB_DF3, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100), "B": datetime.timedelta(days=1)},
        {"A": datetime.datetime(2023, 6, 8, 11, 21, 31, 101), "B": datetime.timedelta(days=2)},
        {},
    ])],

    [DateTimeAddRule, "A", "B", "days", None, DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "Int64"}), DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "Int64"})],
    [DateTimeSubstractRule, "A", "B", "days", None, DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "Int64"}), DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "Int64"})],

    [DateTimeAddRule, "A", "B", "days", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 12, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 12, 11, 21, 31, 101), "B": 2},
        {},
    ])],
    [DateTimeSubstractRule, "A", "B", "days", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 8, 11, 21, 31, 101), "B": 2},
        {},
    ])],

    [DateTimeAddRule, "A", "B", "weekdays", None, DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "Int64"}), DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "Int64"})],
    [DateTimeSubstractRule, "A", "B", "weekdays", None, DataFrame(data={"A": [], "B": []}, dtype="datetime64[ns]"), DataFrame(data={"A": [], "B": []}).astype({"A": "timedelta64[ns]", "B": "datetime64[ns]"})],

    [DateTimeAddRule, "A", 10, "weekdays", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 25, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 23, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 10, "weekdays", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 4, 27, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 5, 29, 11, 21, 31, 101)},
        {},
    ])],

    [DateTimeAddRule, "A", 40, "years", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2063, 5, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(2063, 6, 10, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", 5, "months", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 10, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 11, 10, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeAddRule, "A", 3, "weeks", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 6, 1, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 7, 1, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 40, "years", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(1983, 5, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(1983, 6, 10, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 5, "months", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2022, 12, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 1, 10, 11, 21, 31, 101)},
        {},
    ])],
    [DateTimeSubstractRule, "A", 3, "weeks", None, INPUT_ADD_SUB_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 4, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 5, 20, 11, 21, 31, 101)},
        {},
    ])],

    [DateTimeAddRule, "A", "B", "weekdays", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 12, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 13, 11, 21, 31, 101), "B": 2},
        {},
    ])],
    [DateTimeSubstractRule, "A", "B", "weekdays", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 8, 11, 21, 31, 101), "B": 2},
        {},
    ])],

    [DateTimeAddRule, "A", "B", "years", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2024, 5, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2025, 6, 10, 11, 21, 31, 101), "B": 2},
        {},
    ])],
    [DateTimeAddRule, "A", "B", "months", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 6, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 8, 10, 11, 21, 31, 101), "B": 2},
        {},
    ])],
    [DateTimeAddRule, "A", "B", "weeks", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 18, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 24, 11, 21, 31, 101), "B": 2},
        {},
    ])],
    [DateTimeSubstractRule, "A", "B", "years", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2022, 5, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2021, 6, 10, 11, 21, 31, 101), "B": 2},
        {},
    ])],
    [DateTimeSubstractRule, "A", "B", "months", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 4, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 4, 10, 11, 21, 31, 101), "B": 2},
        {},
    ])],
    [DateTimeSubstractRule, "A", "B", "weeks", None, INPUT_ADD_SUB_DF4, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 4, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 5, 27, 11, 21, 31, 101), "B": 2},
        {},
    ])],

    [DateTimeAddRule, "B", 10, "days", None, INPUT_ADD_SUB_DF, MissingColumnError],
    [DateTimeAddRule, "A", 10, "days", "B", INPUT_ADD_SUB_DF2, ColumnAlreadyExistsError],
    [DateTimeAddRule, "A", "C", "days", None, INPUT_ADD_SUB_DF2, MissingColumnError],
    [DateTimeSubstractRule, "B", 10, "days", None, INPUT_ADD_SUB_DF, MissingColumnError],
    [DateTimeSubstractRule, "A", 10, "days", "B", INPUT_ADD_SUB_DF2, ColumnAlreadyExistsError],
    [DateTimeSubstractRule, "A", "C", "days", None, INPUT_ADD_SUB_DF2, MissingColumnError],])
def test_add_sub_rules(rule_cls, input_column, unit_value, unit, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(
            input_column, unit_value, unit, output_column,
            named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_DATE_DIFF_DF = DataFrame(data=[
    {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
    {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
    {"A": datetime.datetime(2023, 5, 5, 10, 0, 0)},
])

@pytest.mark.parametrize("input_column, input_column2, unit, output_column, input_df, expected", [
    ["A", "B", "days", None, DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "datetime64[ns]"}), DataFrame(data={"A": [], "B": []}).astype({"A": "Int64", "B": "datetime64[ns]"})],
    ["A", "B", "days", None, INPUT_DATE_DIFF_DF, DataFrame(data=[
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 1, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "B", "days", "E", INPUT_DATE_DIFF_DF, DataFrame(data=[
        {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 1), "E": 0},
        {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 0), "E": 1},
        {"A": datetime.datetime(2023, 5, 5, 10, 0, 0)},
    ]).astype({"E": "Int64"})],
    ["A", "B", "hours", None, INPUT_DATE_DIFF_DF, DataFrame(data=[
        {"A": 23, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "B", "minutes", None, INPUT_DATE_DIFF_DF, DataFrame(data=[
        {"A": 59, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "B", "seconds", None, INPUT_DATE_DIFF_DF, DataFrame(data=[
        {"A": 59, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "B", "total_seconds", None, INPUT_DATE_DIFF_DF, DataFrame(data=[
        {"A": 86399, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 86400, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ]).astype({"A": "Int64"})],
    ["A", "B", "total_seconds", None, DataFrame(data={"A": [], "B": []}).astype({"A": "datetime64[ns]", "B": "datetime64[ns]"}), DataFrame(data={"A": [], "B": []}).astype({"A": "Int64", "B": "datetime64[ns]"})],
    
    ["A", "Z", "days", None, INPUT_DATE_DIFF_DF, MissingColumnError],
    ["Z", "B", "days", None, INPUT_DATE_DIFF_DF, MissingColumnError],
    ["A", "B", "days", "A", INPUT_DATE_DIFF_DF, ColumnAlreadyExistsError],
])
def test_date_diff_scenarios(input_column, input_column2, unit, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = DateTimeDiffRule(
            input_column, input_column2, unit, output_column,
            named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False