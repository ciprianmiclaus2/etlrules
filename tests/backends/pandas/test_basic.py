from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest
from etlrules.backends.pandas import DedupeRule, ProjectRule, RenameRule
from etlrules.exceptions import MissingColumnError
from tests.backends.pandas.utils.data import get_test_data


@pytest.mark.parametrize("columns,exclude,main_input,named_inputs,named_input,named_output,expected", [
    [["A", "C", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, None, DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])],
    [["A", "C", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, "result", DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])],
    [["F", "C", "A", "D", "B", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, None, DataFrame(data=[{"F": "f", "C": 3, "A": 1, "D": 4, "B": "b", "E": "e"}])],
    [["F", "C", "A", "D", "B", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, "result", DataFrame(data=[{"F": "f", "C": 3, "A": 1, "D": 4, "B": "b", "E": "e"}])],
    [["A", "C", "E"], True, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, None, DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])],
    [["A", "C", "E"], True, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, "result", DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])],
    [["A", "C", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), {"second_df": DataFrame(data=[{"A": 12, "B": "b2", "C": 32, "D": 42, "E": "e2", "F": "f2"}])}, "second_df", None, DataFrame(data=[{"A": 12, "C": 32, "E": "e2"}])],
])
def test_project_rule_scenarios(columns, exclude, main_input, named_inputs, named_input, named_output, expected):
    with get_test_data(main_input, named_inputs=named_inputs, named_output=named_output) as data:
        rule = ProjectRule(columns, exclude=exclude, named_input=named_input, named_output=named_output)
        rule.apply(data)
        result = data.get_named_output(named_output) if named_output else data.get_main_output()
        assert_frame_equal(result, expected)


def test_project_rule_unknown_column_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = ProjectRule(["A", "C", "UNKNOWN", "E"])
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_project_rule_unknown_column_not_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = ProjectRule(["A", "C", "UNKNOWN", "E"], strict=False)
        rule.apply(data)
        expected = DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_unknown_column_exclude_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = ProjectRule(["A", "C", "UNKNOWN", "E"], exclude=True)
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_project_rule_unknown_column_exclude_not_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = ProjectRule(["A", "C", "UNKNOWN", "E"], exclude=True, strict=False)
        rule.apply(data)
        expected = DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_name_description():
    rule = ProjectRule(["A", "C", "E"], name="Rule 1", description="This is the documentation for the rule")
    assert rule.rule_name() == "Rule 1"
    assert rule.rule_description() == "This is the documentation for the rule"


def test_rename_rule():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df) as data:
        rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'})
        rule.apply(data)
        expected = DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
        assert_frame_equal(data.get_main_output(), expected)

def test_rename_rule_named_input():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df, named_inputs={'other_data': df}) as data:
        rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'}, named_input='other_data', named_output="result")
        rule.apply(data)
        expected = DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_rename_rule_strict_unknown_column():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df) as data:
        rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'})
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_rename_rule_non_strict_unknown_column():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df) as data:
        rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'}, strict=False)
        rule.apply(data)
        expected = DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_rename_rule_name_description():
    rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'}, name="Rule 1", description="This is the documentation for the rule")
    assert rule.rule_name() == "Rule 1"
    assert rule.rule_description() == "This is the documentation for the rule"


DEDUPE_KEEP_FIRST_INPUT_DF = DataFrame(data=[
    {"A": 1, "B": 1, "C": 1},
    {"A": 1, "B": 1, "C": 2},
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
])
DEDUPE_KEEP_FIRST_EXPECTED_DF = DataFrame(data=[
    {"A": 1, "B": 1, "C": 1},
    {"A": 2, "B": 3, "C": 4},
])
DEDUPE_KEEP_LAST_INPUT_DF = DataFrame(data=[
    {"A": 1, "B": 1, "C": 1},
    {"A": 1, "B": 1, "C": 2},
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
])
DEDUPE_KEEP_LAST_EXPECTED_DF = DataFrame(data=[
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
])
DEDUPE_KEEP_NONE_INPUT_DF = DataFrame(data=[
    {"A": 1, "B": 1, "C": 1},
    {"A": 1, "B": 1, "C": 2},
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
])
DEDUPE_KEEP_NONE_EXPECTED_DF = DataFrame(data=[
    {"A": 2, "B": 3, "C": 4},
])


@pytest.mark.parametrize("columns,keep,input_df,named_input,named_output,expected", [
    [["A", "B"], "first", DEDUPE_KEEP_FIRST_INPUT_DF, None, None, DEDUPE_KEEP_FIRST_EXPECTED_DF],
    [["A", "B"], "first", DEDUPE_KEEP_FIRST_INPUT_DF, "input_df", None, DEDUPE_KEEP_FIRST_EXPECTED_DF],
    [["A", "B"], "first", DEDUPE_KEEP_FIRST_INPUT_DF, "input_df", "result", DEDUPE_KEEP_FIRST_EXPECTED_DF],
    [["A", "B"], "last", DEDUPE_KEEP_LAST_INPUT_DF, None, None, DEDUPE_KEEP_LAST_EXPECTED_DF],
    [["A", "B"], "last", DEDUPE_KEEP_LAST_INPUT_DF, "input_df", None, DEDUPE_KEEP_LAST_EXPECTED_DF],
    [["A", "B"], "last", DEDUPE_KEEP_LAST_INPUT_DF, "input_df", "result", DEDUPE_KEEP_LAST_EXPECTED_DF],
    [["A", "B"], "none", DEDUPE_KEEP_NONE_INPUT_DF, None, None, DEDUPE_KEEP_NONE_EXPECTED_DF],
    [["A", "B"], "none", DEDUPE_KEEP_NONE_INPUT_DF, "input_df", None, DEDUPE_KEEP_NONE_EXPECTED_DF],
    [["A", "B"], "none", DEDUPE_KEEP_NONE_INPUT_DF, "input_df", "result", DEDUPE_KEEP_NONE_EXPECTED_DF],
])
def test_dedupe_rule_first(columns,keep,input_df,named_input,named_output,expected):
    with get_test_data(main_input=input_df, named_inputs=named_input and {named_input: input_df}, named_output=named_output) as data:
        rule = DedupeRule(columns, keep=keep, named_output=named_output)
        rule.apply(data)
        assert_frame_equal(data.get_main_output() if named_output is None else data.get_named_output(named_output), expected)


def test_dedupe_rule_raises_missing_column():
    df = DataFrame(data=[
        {"A": 1, "B": 1, "C": 1},
        {"A": 1, "B": 1, "C": 2},
        {"A": 2, "B": 3, "C": 4},
        {"A": 1, "B": 1, "C": 3},
    ])
    with get_test_data(df) as data:
        rule = DedupeRule(["A", "B", "D"], keep='first', strict=False)
        with pytest.raises(MissingColumnError):
            rule.apply(data)
