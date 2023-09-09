from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest
from finrules.data import RuleData
from finrules.backends.pandas import StartRule, ProjectRule


def test_start_rule_main_input():
    main_input = DataFrame(data=[{"A": 1, "B": "b"}])
    data = RuleData()
    rule = StartRule(main_input)
    rule.apply(data)
    assert_frame_equal(data.get_main_output(), main_input)


def test_start_rule_main_input_and_named_inputs():
    main_input = DataFrame(data=[{"A": 1, "B": "b"}])
    named1 = DataFrame(data=[{"C": 2, "D": "d"}])
    named2 = DataFrame(data=[{"D": 3, "E": "e"}])
    data = RuleData()
    rule = StartRule(main_input, named_inputs={"named1": named1, "named2": named2})
    rule.apply(data)
    assert_frame_equal(data.get_main_output(), main_input)
    assert_frame_equal(data.get_named_output("named1"), named1)
    assert_frame_equal(data.get_named_output("named2"), named2)


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
    with pytest.raises(AssertionError):
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
    with pytest.raises(AssertionError):
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
