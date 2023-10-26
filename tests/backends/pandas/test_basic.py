from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest
from etlrules.backends.pandas import DedupeRule, ProjectRule, RenameRule, ReplaceRule
from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from tests.backends.pandas.utils.data import get_test_data


@pytest.mark.parametrize("columns,exclude,main_input,named_inputs,named_input,named_output,expected", [
    [["A", "C", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, None, DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])],
    [["A", "C", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, "result", DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])],
    [["F", "C", "A", "D", "B", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, None, DataFrame(data=[{"F": "f", "C": 3, "A": 1, "D": 4, "B": "b", "E": "e"}])],
    [["F", "C", "A", "D", "B", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, "result", DataFrame(data=[{"F": "f", "C": 3, "A": 1, "D": 4, "B": "b", "E": "e"}])],
    [["A", "C", "E"], True, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, None, DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])],
    [["A", "C", "E"], True, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), None, None, "result", DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])],
    [["A", "C", "E"], False, DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}]), {"second_df": DataFrame(data=[{"A": 12, "B": "b2", "C": 32, "D": 42, "E": "e2", "F": "f2"}])}, "second_df", None, DataFrame(data=[{"A": 12, "C": 32, "E": "e2"}])],
    [["A", "C", "E"], False, DataFrame(data={"A": [], "B": [], "C": [], "D": [], "E": [], "F": []}), None, None, None, DataFrame(data={"A": [], "C": [], "E": []})],
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
    assert rule.get_name() == "Rule 1"
    assert rule.get_description() == "This is the documentation for the rule"


def test_rename_rule():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df) as data:
        rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'})
        rule.apply(data)
        expected = DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_rename_rule_empty_df():
    df = DataFrame(data={"A": [], "B": [], "C": [], "D": [], "E": [], "F": []})
    with get_test_data(df) as data:
        rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'})
        rule.apply(data)
        expected = DataFrame(data={"AA": [], "B": [], "CC": [], "D": [], "EE": [], "F": []})
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
    assert rule.get_name() == "Rule 1"
    assert rule.get_description() == "This is the documentation for the rule"


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
DEDUPE_EMPTY_DF = DataFrame(data={"A": [], "B": [], "C": []})


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
    [["A", "B"], "first", DEDUPE_EMPTY_DF, None, None, DEDUPE_EMPTY_DF],
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


@pytest.mark.parametrize("input_column,values,new_values,regex,output_column,input_df,expected", [
    ["A", ["a", 1], ["new_a", 2], False, None, 
        DataFrame(data=[{"A": "a", "B": 3}, {"A": "aa", "B": 1}]),
        DataFrame(data=[{"A": "new_a", "B": 3}, {"A": "aa", "B": 1}])
    ],
    ["A", ["a", "aa"], ["new_a", "new_aa"], False, None, 
        DataFrame(data=[{"A": "a", "B": 3}, {"A": "aa", "B": 1}]),
        DataFrame(data=[{"A": "new_a", "B": 3}, {"A": "new_aa", "B": 1}])
    ],
    ["B", ["a", 1], ["new_a", 2], False, "F", 
        DataFrame(data=[{"A": "a", "B": 3}, {"A": "aa", "B": 1}]),
        DataFrame(data=[{"A": "a", "B": 3, "F": 3}, {"A": "aa", "B": 1, "F": 2}]),
    ],
    ["A", ["a.*d"], ["new_a"], True, None, 
        DataFrame(data=[{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}]),
        DataFrame(data=[{"A": "new_a", "B": 3}, {"A": "new_a", "B": 1}])
    ],
    ["A", [r"a(.*)d"], [r"new_\1_"], True, None, 
        DataFrame(data=[{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}]),
        DataFrame(data=[{"A": "new_gag_", "B": 3}, {"A": "new_a_", "B": 1}])
    ],
    ["Z", ["a", 1], ["new_a", 2], False, None, 
        DataFrame(data=[{"A": "a", "B": 3}, {"A": "aa", "B": 1}]),
        MissingColumnError
    ],
    ["A", ["a", 1], ["new_a", 2], False, "A", 
        DataFrame(data=[{"A": "a", "B": 3}, {"A": "aa", "B": 1}]),
        ColumnAlreadyExistsError
    ],
    ["A", ["a", 1], ["new_a", 2], False, None, 
        DataFrame(data={"A": [], "B": []}),
        DataFrame(data={"A": [], "B": []}),
    ],
])
def test_replace_scenarios(input_column, values, new_values, regex, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = ReplaceRule(
            input_column=input_column, values=values, new_values=new_values, regex=regex,
            output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False
