from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest
from finrules.data import RuleData
from finrules.backends.pandas import DedupeRule, ProjectRule, RenameRule
from finrules.exceptions import MissingColumn


def test_project_rule_simple():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df)
    rule = ProjectRule(["A", "C", "E"])
    rule.apply(data)
    expected = DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_reorder():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df)
    rule = ProjectRule(["F", "C", "A", "D", "B", "E"])
    rule.apply(data)
    expected = DataFrame(data=[{"F": "f", "C": 3, "A": 1, "D": 4, "B": "b", "E": "e"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_exclude():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df)
    rule = ProjectRule(["A", "C", "E"], exclude=True)
    rule.apply(data)
    expected = DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_unknown_column_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df)
    rule = ProjectRule(["A", "C", "UNKNOWN", "E"])
    with pytest.raises(MissingColumn):
        rule.apply(data)


def test_project_rule_unknown_column_not_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df)
    rule = ProjectRule(["A", "C", "UNKNOWN", "E"], strict=False)
    rule.apply(data)
    expected = DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_unknown_column_exclude_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df)
    rule = ProjectRule(["A", "C", "UNKNOWN", "E"], exclude=True)
    with pytest.raises(MissingColumn):
        rule.apply(data)


def test_project_rule_unknown_column_exclude_not_strict():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df)
    rule = ProjectRule(["A", "C", "UNKNOWN", "E"], exclude=True, strict=False)
    rule.apply(data)
    expected = DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_named_putput():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    df2 = DataFrame(data=[{"A": 12, "B": "b2", "C": 32, "D": 42, "E": "e2", "F": "f2"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    data = RuleData(df, named_inputs={"second_df": df2})
    rule = ProjectRule(["A", "C", "E"], named_input="second_df")
    rule.apply(data)
    expected = DataFrame(data=[{"A": 12, "C": 32, "E": "e2"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_name_description():
    rule = ProjectRule(["A", "C", "E"], name="Rule 1", description="This is the documentation for the rule")
    assert rule.rule_name() == "Rule 1"
    assert rule.rule_description() == "This is the documentation for the rule"


def test_rename_rule():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    data = RuleData(df)
    rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'})
    rule.apply(data)
    expected = DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
    assert_frame_equal(data.get_main_output(), expected)

def test_rename_rule_named_input():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    data = RuleData(df, named_inputs={'other_data': df})
    rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'}, named_input='other_data')
    rule.apply(data)
    expected = DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_rename_rule_strict_unknown_column():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    data = RuleData(df)
    rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'})
    with pytest.raises(MissingColumn):
        rule.apply(data)


def test_rename_rule_non_strict_unknown_column():
    df = DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    data = RuleData(df)
    rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'}, strict=False)
    rule.apply(data)
    expected = DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
    assert_frame_equal(data.get_main_output(), expected)


def test_rename_rule_name_description():
    rule = RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'}, name="Rule 1", description="This is the documentation for the rule")
    assert rule.rule_name() == "Rule 1"
    assert rule.rule_description() == "This is the documentation for the rule"


def test_dedupe_rule_first():
    df = DataFrame(data=[
        {"A": 1, "B": 1, "C": 1},
        {"A": 1, "B": 1, "C": 2},
        {"A": 2, "B": 3, "C": 4},
        {"A": 1, "B": 1, "C": 3},
    ])
    data = RuleData(df)
    rule = DedupeRule(["A", "B"], keep='first', strict=False)
    rule.apply(data)
    expected = DataFrame(data=[
        {"A": 1, "B": 1, "C": 1},
        {"A": 2, "B": 3, "C": 4},
    ])
    assert_frame_equal(data.get_main_output(), expected)


def test_dedupe_rule_last():
    df = DataFrame(data=[
        {"A": 1, "B": 1, "C": 1},
        {"A": 1, "B": 1, "C": 2},
        {"A": 2, "B": 3, "C": 4},
        {"A": 1, "B": 1, "C": 3},
    ])
    data = RuleData(df)
    rule = DedupeRule(["A", "B"], keep='last', strict=False)
    rule.apply(data)
    expected = DataFrame(data=[
        {"A": 2, "B": 3, "C": 4},
        {"A": 1, "B": 1, "C": 3},
    ])
    assert_frame_equal(data.get_main_output(), expected)


def test_dedupe_rule_none():
    df = DataFrame(data=[
        {"A": 1, "B": 1, "C": 1},
        {"A": 1, "B": 1, "C": 2},
        {"A": 2, "B": 3, "C": 4},
        {"A": 1, "B": 1, "C": 3},
    ])
    data = RuleData(df)
    rule = DedupeRule(["A", "B"], keep='none', strict=False)
    rule.apply(data)
    expected = DataFrame(data=[
        {"A": 2, "B": 3, "C": 4},
    ])
    assert_frame_equal(data.get_main_output(), expected)


def test_dedupe_rule_raises_missing_column():
    df = DataFrame(data=[
        {"A": 1, "B": 1, "C": 1},
        {"A": 1, "B": 1, "C": 2},
        {"A": 2, "B": 3, "C": 4},
        {"A": 1, "B": 1, "C": 3},
    ])
    data = RuleData(df)
    rule = DedupeRule(["A", "B", "D"], keep='first', strict=False)
    with pytest.raises(MissingColumn):
        rule.apply(data)
