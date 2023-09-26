from pandas import DataFrame
from pandas.testing import assert_frame_equal

from etlrules.backends.pandas import SortRule
from tests.backends.pandas.utils.data import get_test_data


def test_sort_rule_single_column():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 9, "B": "b", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    with get_test_data(df) as data:
        rule = SortRule("A")
        rule.apply(data)
        expected = DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "b", "C": 3}, {"A": 9, "B": "b", "C": 2}])
        assert_frame_equal(data.get_main_output(), expected)


def test_sort_rule_multi_columns():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    with get_test_data(df, named_output="result") as data:
        rule = SortRule(["A", "B", "C"], named_output="result")
        rule.apply(data)
        expected = DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "a", "C": 2}, {"A": 5, "B": "b", "C": 3}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_sort_rule_multi_columns_descending():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    with get_test_data(df, named_output="result") as data:
        rule = SortRule(["A", "B", "C"], ascending=False, named_output="result")
        rule.apply(data)
        expected = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_sort_rule_multi_columns_ascending_mixed():
    df = DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    with get_test_data(df, named_output="result") as data:
        rule = SortRule(["A", "B", "C"], ascending=[True, False, True], named_output="result")
        rule.apply(data)
        expected = DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_name_description():
    rule = SortRule(["A", "B", "C"], name="Rule 1", description="This is the documentation for the rule")
    assert rule.rule_name() == "Rule 1"
    assert rule.rule_description() == "This is the documentation for the rule"
