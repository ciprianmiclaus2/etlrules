import datetime
import polars as pl
import pytest
import sys

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from tests.utils.data import assert_frame_equal, get_test_data


def test_utcnow_rule(backend):
    input_df = backend.DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeUTCNowRule(output_column='TimeNow', named_input="input", named_output="result")
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A", "TimeNow"]
        assert all((x - datetime.datetime.utcnow()).total_seconds() < 5 for x in result["TimeNow"])


def test_utcnow_existing_column_strict(backend):
    input_df = backend.DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeUTCNowRule(output_column='A', named_input="input", named_output="result")
        with pytest.raises(ColumnAlreadyExistsError):
            rule.apply(data)


def test_utcnow_existing_column_non_strict(backend):
    input_df = backend.DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeUTCNowRule(output_column='A', named_input="input", named_output="result", strict=False)
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A"]
        assert all((x - datetime.datetime.now()).total_seconds() < 5 for x in result["A"])


def test_localnow_rule(backend):
    input_df = backend.DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeLocalNowRule(output_column='TimeNow', named_input="input", named_output="result")
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A", "TimeNow"]
        assert all((x - datetime.datetime.now()).total_seconds() < 5 for x in result["TimeNow"])


def test_localnow_existing_column_strict(backend):
    input_df = backend.DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeLocalNowRule(output_column='A', named_input="input", named_output="result")
        with pytest.raises(ColumnAlreadyExistsError):
            rule.apply(data)


def test_localnow_existing_column_non_strict(backend):
    input_df = backend.DataFrame(data=[
        {"A": 1},
        {"A": 2},
        {"A": 3},
    ])
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeLocalNowRule(output_column='A', named_input="input", named_output="result", strict=False)
        rule.apply(data)
        result = data.get_named_output("result")
        assert list(result.columns) == ["A"]
        assert all((x - datetime.datetime.now()).total_seconds() < 5 for x in result["A"])


@pytest.mark.parametrize("input_column,format,output_column,input_df,input_astype,expected,expected_astype", [
    ["A", "%Y-%m-%d %H:%M:%S", None, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ], None, [
        {"A": "2023-05-15 09:15:45"},
        {"A": "2023-05-16 19:25:00"},
    ], {"A": "string"}],
    ["A", "%Y-%m-%d %H:%M:%S", None, {"A": []}, {"A": "datetime"}, {"A": []}, {"A": "string"}],
    ["A", "%Y-%m-%d %H:%M", None, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ], None, [
        {"A": "2023-05-15 09:15", "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": "2023-05-16 19:25"},
    ], {"A": "string"}],
    ["A", "%Y-%m-%d %H:%M", "E", [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ], None, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45), "E": "2023-05-15 09:15"},
        {"A": datetime.datetime(2023, 5, 16, 19, 25), "E": "2023-05-16 19:25"},
    ], {"E": "string"}],
    ["Z", "%Y-%m-%d %H:%M", "E", [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ], None, MissingColumnError, None],
    ["A", "%Y-%m-%d %H:%M", "B", [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45), "B": datetime.datetime(2023, 7, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    ], None, ColumnAlreadyExistsError, None],
])
def test_str_format(input_column, format, output_column, input_df, input_astype, expected, expected_astype, backend):
    input_df = backend.DataFrame(input_df, astype=input_astype)
    if isinstance(expected, (list, dict)):
        expected = backend.DataFrame(expected, astype=expected_astype)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeToStrFormatRule(
            input_column, format=format,
            output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_DF = [
    {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
    {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
    {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
    {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1),}
]


def is_empty_df(df):
    if isinstance(df, dict):
        return not df or all(not x for x in df.values())
    elif isinstance(df, list):
        return not df or all(not x for x in df)
    assert False, f"unexpected param passed into is_empty_df: {df}"


@pytest.mark.parametrize("rule_cls_str, input_column, unit, output_column, input_df, expected", [
    ["DateTimeRoundRule", "A", "day", None, {"A": []}, {"A": []}],
    ["DateTimeRoundDownRule", "A", "day", None, {"A": []}, {"A": []}],
    ["DateTimeRoundUpRule", "A", "day", None, {"A": []}, {"A": []}],
    ["DateTimeRoundRule", "A", "day", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 16)},
    ]],
    ["DateTimeRoundRule", "A", "day", "E", INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999), "E": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999), "E": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25), "E": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0), "E": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1), "E": datetime.datetime(2023, 7, 16)},
    ]],
    ["DateTimeRoundRule", "A", "hour", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9)},
        {"A": datetime.datetime(2023, 7, 15, 10)},
        {"A": datetime.datetime(2023, 5, 16, 19)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ]],
    ["DateTimeRoundRule", "A", "minute", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 16)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0)},
    ]],
    ["DateTimeRoundRule", "A", "second", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ]],
    ["DateTimeRoundRule", "A", "millisecond", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 10000)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 100000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ]],
    ["DateTimeRoundRule", "A", "microsecond", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ]],
    #["DateTimeRoundRule", "A", "nanosecond", None, INPUT_DF, [
    #    {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
    #    {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
    #    {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    #    {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
    #    {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    #]],

    ["DateTimeRoundDownRule", "A", "day", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15)},
        {"A": datetime.datetime(2023, 5, 16)},
        {"A": datetime.datetime(2023, 5, 15)},
        {"A": datetime.datetime(2023, 7, 15)},
    ]],
    ["DateTimeRoundDownRule", "A", "hour", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9)},
        {"A": datetime.datetime(2023, 7, 15, 9)},
        {"A": datetime.datetime(2023, 5, 16, 19)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ]],
    ["DateTimeRoundDownRule", "A", "minute", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ]],
    ["DateTimeRoundDownRule", "A", "second", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12)},
    ]],
    ["DateTimeRoundDownRule", "A", "millisecond", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9000)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0)},
    ]],
    ["DateTimeRoundDownRule", "A", "microsecond", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ]],
    #["DateTimeRoundDownRule", "A", "nanosecond", None, INPUT_DF, [
    #    {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
    #    {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
    #    {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    #    {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
    #    {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    #]],

    ["DateTimeRoundUpRule", "A", "day", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 16)},
        {"A": datetime.datetime(2023, 7, 16)},
        {"A": datetime.datetime(2023, 5, 17)},
        {"A": datetime.datetime(2023, 5, 16)},
        {"A": datetime.datetime(2023, 7, 16)},
    ]],
    ["DateTimeRoundUpRule", "A", "hour", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 10)},
        {"A": datetime.datetime(2023, 7, 15, 10)},
        {"A": datetime.datetime(2023, 5, 16, 20)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 13)},
    ]],
    ["DateTimeRoundUpRule", "A", "minute", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 16)},
        {"A": datetime.datetime(2023, 7, 15, 9, 46)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12)},
        {"A": datetime.datetime(2023, 7, 15, 12, 1)},
    ]],
    ["DateTimeRoundUpRule", "A", "second", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 46)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 16)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 1)},
    ]],
    ["DateTimeRoundUpRule", "A", "millisecond", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 10000)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 100000)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1000)},
    ]],
    ["DateTimeRoundUpRule", "A", "microsecond", None, INPUT_DF, [
        {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
        {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
        {"A": datetime.datetime(2023, 5, 16, 19, 25)},
        {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
        {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1)},
    ]],
    #["DateTimeRoundUpRule", "A", "nanosecond", None, INPUT_DF, [
    #    {"A": datetime.datetime(2023, 5, 15, 9, 15, 45, 9999)},
    #    {"A": datetime.datetime(2023, 7, 15, 9, 45, 15, 99999)},
    #    {"A": datetime.datetime(2023, 5, 16, 19, 25)},
    #    {"A": datetime.datetime(2023, 5, 15, 12, 0, 0)},
    #    {"A": datetime.datetime(2023, 7, 15, 12, 0, 0, 1000)},
    #]],

    ["DateTimeRoundRule", "Z", "day", None, INPUT_DF, MissingColumnError],
    ["DateTimeRoundRule", "A", "day", "A", INPUT_DF, ColumnAlreadyExistsError],
    ["DateTimeRoundDownRule", "Z", "day", None, INPUT_DF, MissingColumnError],
    ["DateTimeRoundDownRule", "A", "day", "A", INPUT_DF, ColumnAlreadyExistsError],
    ["DateTimeRoundUpRule", "Z", "day", None, INPUT_DF, MissingColumnError],
    ["DateTimeRoundUpRule", "A", "day", "A", INPUT_DF, ColumnAlreadyExistsError],
])
def test_round_trunc_rules(rule_cls_str, input_column, unit, output_column, input_df, expected, backend):
    input_df = backend.DataFrame(input_df, dtype="datetime" if is_empty_df(input_df) else None)
    if isinstance(expected, (list, dict)):
        expected = backend.DataFrame(expected, dtype="datetime" if is_empty_df(expected) else None)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule_cls = getattr(backend.rules, rule_cls_str)
        rule = rule_cls(
            input_column, unit, output_column=output_column,
            named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            actual = data.get_named_output("result")
            assert_frame_equal(actual, expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_COMPONENT_DF = [
    {"A": datetime.datetime(2023, 5, 10, 19, 15, 45, 999)},
    {"A": datetime.datetime(2023, 6, 11, 9, 35, 15, 777)},
    {},
]

INPUT_COMPONENT_DF2 = [
    {"A": datetime.datetime(2023, 5, 10, 19, 15, 45, 999), "B": 1},
    {"A": datetime.datetime(2023, 6, 11, 9, 35, 15, 777), "B": 2},
    {},
]

@pytest.mark.parametrize("input_column,component,locale,output_column,input_df,expected,expected_astype", [
    ["A", "year", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "year", None, None, INPUT_COMPONENT_DF, [
        {"A": 2023},
        {"A": 2023},
        {},
    ], {"A": "Int64"}],
    ["A", "year", None, "E", INPUT_COMPONENT_DF, [
        {"A": datetime.datetime(2023, 5, 10, 19, 15, 45, 999), "E": 2023},
        {"A": datetime.datetime(2023, 6, 11, 9, 35, 15, 777), "E": 2023},
        {},
    ], {"E": "Int64"}],
    ["A", "month", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "month", None, None, INPUT_COMPONENT_DF, [
        {"A": 5},
        {"A": 6},
        {},
    ], {"A": "Int64"}],
    ["A", "day", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "day", None, None, INPUT_COMPONENT_DF, [
        {"A": 10},
        {"A": 11},
        {},
    ], {"A": "Int64"}],
    ["A", "hour", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "hour", None, None, INPUT_COMPONENT_DF, [
        {"A": 19},
        {"A": 9},
        {},
    ], {"A": "Int64"}],
    ["A", "minute", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "minute", None, None, INPUT_COMPONENT_DF, [
        {"A": 15},
        {"A": 35},
        {},
    ], {"A": "Int64"}],
    ["A", "second", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "second", None, None, INPUT_COMPONENT_DF, [
        {"A": 45},
        {"A": 15},
        {},
    ], {"A": "Int64"}],
    ["A", "microsecond", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "microsecond", None, None, INPUT_COMPONENT_DF, [
        {"A": 999},
        {"A": 777},
        {},
    ], {"A": "Int64"}],
    ["A", "weekday", None, None, {"A": []}, {"A": []}, {"A": "Int64"}],
    ["A", "weekday", None, None, INPUT_COMPONENT_DF, [
        {"A": 2},
        {"A": 6},
        {},
    ], {"A": "Int64"}],
    ["A", "day_name", None, None, {"A": []}, {"A": []}, {"A": "string"}],
    ["A", "day_name", None, None, INPUT_COMPONENT_DF, [
        {"A": "Wednesday"},
        {"A": "Sunday"},
        {},
    ], {"A": "string"}],
    ["A", "day_name", "en_US.utf8", None, INPUT_COMPONENT_DF, [
        {"A": "Wednesday"},
        {"A": "Sunday"},
        {},
    ], {"A": "string"}],
    ["A", "month_name", None, None, {"A": []}, {"A": []}, {"A": "string"}],
    ["A", "month_name", None, None, INPUT_COMPONENT_DF, [
        {"A": "May"},
        {"A": "June"},
        {},
    ], {"A": "string"}],
    ["A", "day_name", "UNKNOWN_LOCALE", None, INPUT_COMPONENT_DF, ValueError, None],
    ["Z", "year", None, None, INPUT_COMPONENT_DF, MissingColumnError, None],
    ["A", "year", None, "B", INPUT_COMPONENT_DF2, ColumnAlreadyExistsError, None],
])
def test_extract_component_rules(input_column, component, locale, output_column, input_df, expected, expected_astype, backend):
    input_df = backend.DataFrame(input_df, dtype="datetime" if is_empty_df(input_df) else None)
    if isinstance(expected, (list, dict)):
        expected = backend.DataFrame(expected, astype=expected_astype)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, backend.impl.DataFrame):
            rule = backend.rules.DateTimeExtractComponentRule(
                input_column, component, locale,
                output_column=output_column, named_input="input", named_output="result")
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule = backend.rules.DateTimeExtractComponentRule(
                    input_column, component, locale,
                    output_column=output_column, named_input="input", named_output="result")
                rule.apply(data)
        else:
            assert False


INPUT_ADD_SUB_DF = [
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100)},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101)},
    {},
]

INPUT_ADD_SUB_DF2 = [
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "B": 1},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "B": 2},
    {},
]

INPUT_ADD_SUB_DF3 = [
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "B": datetime.timedelta(days=1)},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "B": datetime.timedelta(days=2)},
    {},
]

INPUT_ADD_SUB_DF4 = [
    {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "B": 1},
    {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "B": 2},
    {},
]


@pytest.mark.parametrize("rule_cls_str, input_column, unit_value, unit, output_column, input_df, input_astype, expected, expected_astype", [
    ["DateTimeAddRule", "A", 40, "days", None, {"A": []}, {"A": "datetime"}, {"A": []}, {"A": "datetime"}],
    ["DateTimeAddRule", "A", 40, "days", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 6, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 7, 20, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", 40, "days", "E", INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 100), "E": datetime.datetime(2023, 6, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 101), "E": datetime.datetime(2023, 7, 20, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", -1, "days", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 9, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 40, "days", None, {"A": []}, {"A": "datetime"}, {"A": []}, {"A": "datetime"}],
    ["DateTimeSubstractRule", "A", -40, "days", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 6, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 7, 20, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 1, "days", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 9, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", 40, "hours", None, {"A": []}, {"A": "datetime"}, {"A": []}, {"A": "datetime"}],
    ["DateTimeAddRule", "A", 40, "hours", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 13, 2, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 12, 3, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 40, "hours", None, {"A": []}, {"A": "datetime"}, {"A": []}, {"A": "datetime"}],
    ["DateTimeSubstractRule", "A", 40, "hours", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 9, 18, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 8, 19, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", 10, "minutes", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 11, 10, 30, 30, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 31, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 10, "minutes", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 11, 10, 10, 30, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 11, 31, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", 10, "seconds", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 40, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 41, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 10, "seconds", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 20, 100)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 21, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", 10, "microseconds", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 110)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 111)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 10, "microseconds", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 11, 10, 20, 30, 90)},
        {"A": datetime.datetime(2023, 6, 10, 11, 21, 31, 91)},
        {},
    ], None],

    ["DateTimeSubstractRule", "A", "B", None, None, {"A": [], "B": []}, {"A": "datetime", "B": "datetime"}, {"A": [], "B": []}, {"A": "timedelta", "B": "datetime"}],

    ["DateTimeAddRule", "A", "B", None, None, INPUT_ADD_SUB_DF3, None, [
        {"A": datetime.datetime(2023, 5, 12, 10, 20, 30, 100), "B": datetime.timedelta(days=1)},
        {"A": datetime.datetime(2023, 6, 12, 11, 21, 31, 101), "B": datetime.timedelta(days=2)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", "B", None, None, INPUT_ADD_SUB_DF3, None, [
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100), "B": datetime.timedelta(days=1)},
        {"A": datetime.datetime(2023, 6, 8, 11, 21, 31, 101), "B": datetime.timedelta(days=2)},
        {},
    ], None],

    ["DateTimeAddRule", "A", "B", "days", None, {"A": [], "B": []}, {"A": "datetime", "B": "Int64"}, {"A": [], "B": []}, {"A": "datetime", "B": "Int64"}],
    ["DateTimeSubstractRule", "A", "B", "days", None, {"A": [], "B": []}, {"A": "datetime", "B": "Int64"}, {"A": [], "B": []}, {"A": "datetime", "B": "Int64"}],

    ["DateTimeAddRule", "A", "B", "days", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 5, 12, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 12, 11, 21, 31, 101), "B": 2},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", "B", "days", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 8, 11, 21, 31, 101), "B": 2},
        {},
    ], None],

    ["DateTimeAddRule", "A", "B", "weekdays", None, {"A": [], "B": []}, {"A": "datetime", "B": "Int64"}, {"A": [], "B": []}, {"A": "datetime", "B": "Int64"}],
    ["DateTimeSubstractRule", "A", "B", "weekdays", None, {"A": [], "B": []}, {"A": "datetime", "B": "datetime"}, {"A": [], "B": []}, {"A": "timedelta", "B": "datetime"}],

    ["DateTimeAddRule", "A", 10, "weekdays", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 5, 25, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 6, 23, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 10, "weekdays", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 4, 27, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 5, 29, 11, 21, 31, 101)},
        {},
    ], None],

    ["DateTimeAddRule", "A", 40, "years", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2063, 5, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(2063, 6, 10, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", 5, "months", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 10, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 11, 10, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeAddRule", "A", 3, "weeks", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 6, 1, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 7, 1, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 40, "years", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(1983, 5, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(1983, 6, 10, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 5, "months", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2022, 12, 11, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 1, 10, 11, 21, 31, 101)},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", 3, "weeks", None, INPUT_ADD_SUB_DF, None, [
        {"A": datetime.datetime(2023, 4, 20, 10, 20, 30, 100)},
        {"A": datetime.datetime(2023, 5, 20, 11, 21, 31, 101)},
        {},
    ], None],

    ["DateTimeAddRule", "A", "B", "weekdays", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 5, 12, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 13, 11, 21, 31, 101), "B": 2},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", "B", "weekdays", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 5, 10, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 8, 11, 21, 31, 101), "B": 2},
        {},
    ], None],

    ["DateTimeAddRule", "A", "B", "years", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2024, 5, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2025, 6, 10, 11, 21, 31, 101), "B": 2},
        {},
    ], None],
    ["DateTimeAddRule", "A", "B", "months", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 6, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 8, 10, 11, 21, 31, 101), "B": 2},
        {},
    ], None],
    ["DateTimeAddRule", "A", "B", "weeks", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 5, 18, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 6, 24, 11, 21, 31, 101), "B": 2},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", "B", "years", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2022, 5, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2021, 6, 10, 11, 21, 31, 101), "B": 2},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", "B", "months", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 4, 11, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 4, 10, 11, 21, 31, 101), "B": 2},
        {},
    ], None],
    ["DateTimeSubstractRule", "A", "B", "weeks", None, INPUT_ADD_SUB_DF4, None, [
        {"A": datetime.datetime(2023, 5, 4, 10, 20, 30, 100), "B": 1},
        {"A": datetime.datetime(2023, 5, 27, 11, 21, 31, 101), "B": 2},
        {},
    ], None],

    ["DateTimeAddRule", "B", 10, "days", None, INPUT_ADD_SUB_DF, None, MissingColumnError, None],
    ["DateTimeAddRule", "A", 10, "days", "B", INPUT_ADD_SUB_DF2, None, ColumnAlreadyExistsError, None],
    ["DateTimeAddRule", "A", "C", "days", None, INPUT_ADD_SUB_DF2, None, MissingColumnError, None],
    ["DateTimeSubstractRule", "B", 10, "days", None, INPUT_ADD_SUB_DF, None, MissingColumnError, None],
    ["DateTimeSubstractRule", "A", 10, "days", "B", INPUT_ADD_SUB_DF2, None, ColumnAlreadyExistsError, None],
    ["DateTimeSubstractRule", "A", "C", "days", None, INPUT_ADD_SUB_DF2, None, MissingColumnError, None],])
def test_add_sub_rules(rule_cls_str, input_column, unit_value, unit, output_column, input_df, input_astype, expected, expected_astype, backend):
    if backend.impl == pl and unit == "weekdays" and sys.version_info <= (3, 10):
        pytest.skip()
    input_df = backend.DataFrame(input_df, astype=input_astype)
    if isinstance(expected, (list, dict)):
        expected = backend.DataFrame(expected, astype=expected_astype)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule_cls = getattr(backend.rules, rule_cls_str)
        rule = rule_cls(
            input_column, unit_value, unit, output_column,
            named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_DATE_DIFF_DF = [
    {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
    {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
    {"A": datetime.datetime(2023, 5, 5, 10, 0, 0)},
]

@pytest.mark.parametrize("input_column, input_column2, unit, output_column, input_df, input_astype, expected, expected_astype", [
    ["A", "B", "days", None, {"A": [], "B": []}, {"A": "datetime", "B": "datetime"}, {"A": [], "B": []}, {"A": "Int64", "B": "datetime"}],
    ["A", "B", "days", None, INPUT_DATE_DIFF_DF, None, [
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 1, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ], {"A": "Int64"}],
    ["A", "B", "days", "E", INPUT_DATE_DIFF_DF, None, [
        {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 1), "E": 0},
        {"A": datetime.datetime(2023, 5, 5, 10, 0, 0), "B": datetime.datetime(2023, 5, 4, 10, 0, 0), "E": 1},
        {"A": datetime.datetime(2023, 5, 5, 10, 0, 0)},
    ], {"E": "Int64"}],
    ["A", "B", "hours", None, INPUT_DATE_DIFF_DF, None, [
        {"A": 23, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ], {"A": "Int64"}],
    ["A", "B", "minutes", None, INPUT_DATE_DIFF_DF, None, [
        {"A": 59, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ], {"A": "Int64"}],
    ["A", "B", "seconds", None, INPUT_DATE_DIFF_DF, None, [
        {"A": 59, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 0, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ], {"A": "Int64"}],
    ["A", "B", "total_seconds", None, INPUT_DATE_DIFF_DF, None, [
        {"A": 86399, "B": datetime.datetime(2023, 5, 4, 10, 0, 1)},
        {"A": 86400, "B": datetime.datetime(2023, 5, 4, 10, 0, 0)},
        {},
    ], {"A": "Int64"}],
    ["A", "B", "total_seconds", None, {"A": [], "B": []}, {"A": "datetime", "B": "datetime"}, {"A": [], "B": []}, {"A": "Int64", "B": "datetime"}],

    ["A", "Z", "days", None, INPUT_DATE_DIFF_DF, None, MissingColumnError, None],
    ["Z", "B", "days", None, INPUT_DATE_DIFF_DF, None, MissingColumnError, None],
    ["A", "B", "days", "A", INPUT_DATE_DIFF_DF, None, ColumnAlreadyExistsError, None],
])
def test_date_diff_scenarios(input_column, input_column2, unit, output_column, input_df, input_astype, expected, expected_astype, backend):
    input_df = backend.DataFrame(input_df, astype=input_astype)
    if isinstance(expected, (list, dict)):
        expected = backend.DataFrame(expected, astype=expected_astype)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DateTimeDiffRule(
            input_column, input_column2, unit, output_column,
            named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False
