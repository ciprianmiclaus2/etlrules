import pytest

from etlrules.exceptions import MissingColumnError
from tests.utils.data import assert_frame_equal, get_test_data


def test_sort_rule_single_column(backend):
    df = backend.impl.DataFrame(data=[{"A2": 5, "B": "b", "C": 3}, {"A2": 9, "B": "b", "C": 2}, {"A2": 3, "B": "b", "C": 8}])
    with get_test_data(df) as data:
        rule = backend.rules.SortRule("A2")
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"A2": 3, "B": "b", "C": 8}, {"A2": 5, "B": "b", "C": 3}, {"A2": 9, "B": "b", "C": 2}])
        assert_frame_equal(data.get_main_output(), expected)


def test_sort_rule_multi_columns(backend):
    df = backend.impl.DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    with get_test_data(df, named_output="result") as data:
        rule = backend.rules.SortRule(["A", "B", "C"], named_output="result")
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "a", "C": 2}, {"A": 5, "B": "b", "C": 3}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_sort_rule_multi_columns_descending(backend):
    df = backend.impl.DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    with get_test_data(df, named_output="result") as data:
        rule = backend.rules.SortRule(["A", "B", "C"], ascending=False, named_output="result")
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_sort_rule_multi_columns_ascending_mixed(backend):
    df = backend.impl.DataFrame(data=[{"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}, {"A": 3, "B": "b", "C": 8}])
    with get_test_data(df, named_output="result") as data:
        rule = backend.rules.SortRule(["A", "B", "C"], ascending=[True, False, True], named_output="result")
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"A": 3, "B": "b", "C": 8}, {"A": 5, "B": "b", "C": 3}, {"A": 5, "B": "a", "C": 2}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_sort_rule_missing_column(backend):
    df = backend.impl.DataFrame(data=[{"A2": 5, "B": "b", "C": 3}, {"A2": 9, "B": "b", "C": 2}, {"A2": 3, "B": "b", "C": 8}])
    with get_test_data(df) as data:
        rule = backend.rules.SortRule("A")
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_sort_rule_empty_df(backend):
    df = backend.impl.DataFrame(data={"A": [], "B": [], "C": []})
    with get_test_data(df, named_output="result") as data:
        rule = backend.rules.SortRule(["A", "C"], named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), df)


def test_name_description(backend):
    rule = backend.rules.SortRule(["A", "B", "C"], name="Rule 1", description="This is the documentation for the rule")
    assert rule.get_name() == "Rule 1"
    assert rule.get_description() == "This is the documentation for the rule"
