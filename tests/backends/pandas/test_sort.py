from pandas import DataFrame
from pandas.testing import assert_frame_equal
from finrules.backends.pandas import SortRule
from finrules.data import RuleData


def test_sort_rule_single_column():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 9, "B": "b", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    data = RuleData(df)
    rule = SortRule("A")
    rule.apply(data)
    expected = DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "b", "C": 3}, {"A": 9, "B": "b", "C": 2}])
    assert_frame_equal(data.get_main_output(), expected)


def test_sort_rule_multi_columns():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    data = RuleData(df)
    rule = SortRule(["A", "B", "C"])
    rule.apply(data)
    expected = DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "a", "C": 2}, {"A": 5, "B": "b", "C": 3}])
    assert_frame_equal(data.get_main_output(), expected)


def test_sort_rule_multi_columns_descending():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    data = RuleData(df)
    rule = SortRule(["A", "B", "C"], ascending=False)
    rule.apply(data)
    expected = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    assert_frame_equal(data.get_main_output(), expected)


def test_sort_rule_multi_columns_ascending_mixed():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    data = RuleData(df)
    rule = SortRule(["A", "B", "C"], ascending=[True, False, True])
    rule.apply(data)
    expected = DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}])
    assert_frame_equal(data.get_main_output(), expected)
