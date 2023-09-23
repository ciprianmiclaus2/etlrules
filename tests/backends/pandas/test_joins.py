from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from finrules.exceptions import MissingColumnError
from finrules.backends.pandas import LeftJoinRule, InnerJoinRule, OuterJoinRule, RightJoinRule
from tests.backends.pandas.utils.data import get_test_data


LEFT_DF = DataFrame(data=[
    {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3},
    {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4},
    {"A": 3, "B": "b", "C": 10, "D": "test", "E": 5},
    {"A": 4, "B": "b", "C": 10, "D": "test", "E": 6},
])

RIGHT_DF = DataFrame(data=[
    {"A": 1, "B": "b", "E": 3, "G": "one"},
    {"A": 2, "B": "b", "E": 4, "G": "two"},
    {"A": 5, "B": "b", "E": 7, "G": "three"},
    {"A": 6, "B": "b", "E": 8, "G": "four"},
])


@pytest.mark.parametrize("rule_cls,key_columns_left,key_columns_right,suffixes,expected", [
    [LeftJoinRule, ["A", "B"], None, (None, "_y"), DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3, "E_y": 3, "G": "one"},
        {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4, "E_y": 4, "G": "two"},
        {"A": 3, "B": "b", "C": 10, "D": "test", "E": 5},
        {"A": 4, "B": "b", "C": 10, "D": "test", "E": 6},
    ])],
    [InnerJoinRule, ["A", "B"], None, (None, "_y"), DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3, "E_y": 3, "G": "one"},
        {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4, "E_y": 4, "G": "two"},
    ])],
    [InnerJoinRule, ["A", "B"], ["A", "B"], (None, "_y"), DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3, "E_y": 3, "G": "one"},
        {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4, "E_y": 4, "G": "two"},
    ])],
    [InnerJoinRule, ["A", "B"], ["A", "B"], ("_x", "_y"), DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "test", "E_x": 3, "E_y": 3, "G": "one"},
        {"A": 2, "B": "b", "C": 10, "D": "test", "E_x": 4, "E_y": 4, "G": "two"},
    ])],
    [InnerJoinRule, ["A", "B"], ["E", "B"], (None, "_y"), DataFrame(data=[
        {"A": 3, "B": "b", "C": 10, "D": "test", "E": 5, "A_y": 1, "E_y": 3, "G": "one"},
        {"A": 4, "B": "b", "C": 10, "D": "test", "E": 6, "A_y": 2, "E_y": 4, "G": "two"},
    ])],
    [RightJoinRule, ["A", "B"], None, (None, "_y"), DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3, "E_y": 3, "G": "one"},
        {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4, "E_y": 4, "G": "two"},
        {"A": 5, "B": "b", "E_y": 7, "G": "three"},
        {"A": 6, "B": "b", "E_y": 8, "G": "four"},
    ])],
    [OuterJoinRule, ["A", "B"], None, (None, "_y"), DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3, "E_y": 3, "G": "one"},
        {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4, "E_y": 4, "G": "two"},
        {"A": 3, "B": "b", "C": 10, "D": "test", "E": 5},
        {"A": 4, "B": "b", "C": 10, "D": "test", "E": 6},
        {"A": 5, "B": "b", "E_y": 7, "G": "three"},
        {"A": 6, "B": "b", "E_y": 8, "G": "four"},
    ])],
])
def test_join_scenarios(rule_cls, key_columns_left, key_columns_right, suffixes, expected):
    with get_test_data(LEFT_DF, named_inputs={"right": RIGHT_DF}, named_output="result") as data:
        rule = rule_cls(named_input_left=None, named_input_right="right", key_columns_left=key_columns_left, 
                        key_columns_right=key_columns_right, suffixes=suffixes, named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_raises_missing_column_left():
    with get_test_data(named_inputs={"left": LEFT_DF, "right": RIGHT_DF}) as data:
        rule = LeftJoinRule(named_input_left="left", named_input_right="right",
                        key_columns_left=["A", "Z"], key_columns_right=["A", "B"])
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Missing columns in join in the left dataframe: {'Z'}"


def test_raises_missing_column_right():
    with get_test_data(named_inputs={"left": LEFT_DF, "right": RIGHT_DF}) as data:
        rule = LeftJoinRule(named_input_left="left", named_input_right="right",
                        key_columns_left=["A", "B"], key_columns_right=["A", "Z"])
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Missing columns in join in the right dataframe: {'Z'}"
