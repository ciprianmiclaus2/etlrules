from pandas import DataFrame, NA
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas import LeftJoinRule, InnerJoinRule, OuterJoinRule, RightJoinRule
from tests.backends.pandas.utils.data import get_test_data


LEFT_DF = DataFrame(data=[
    {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3},
    {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4},
    {"A": 3, "B": "b", "C": 10, "D": "test", "E": 5},
    {"A": 4, "B": "b", "C": 10, "D": "test", "E": 6},
])
EMPTY_LEFT_DF = LEFT_DF[:0]

RIGHT_DF = DataFrame(data=[
    {"A": 1, "B": "b", "E": 3, "G": "one"},
    {"A": 2, "B": "b", "E": 4, "G": "two"},
    {"A": 5, "B": "b", "E": 7, "G": "three"},
    {"A": 6, "B": "b", "E": 8, "G": "four"},
])
EMPTY_RIGHT_DF = RIGHT_DF[:0]


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


@pytest.mark.parametrize("rule_cls,key_columns_left,key_columns_right,suffixes,left_df,right_df,expected", [
    [LeftJoinRule, ["A", "B"], None, (None, "_y"), LEFT_DF, EMPTY_RIGHT_DF, DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "test", "E": 3, "E_y": None, "G": NA},
        {"A": 2, "B": "b", "C": 10, "D": "test", "E": 4},
        {"A": 3, "B": "b", "C": 10, "D": "test", "E": 5},
        {"A": 4, "B": "b", "C": 10, "D": "test", "E": 6},
    ]).astype({"E_y": "float64"})],
    [LeftJoinRule, ["A", "B"], None, (None, "_y"), EMPTY_LEFT_DF, EMPTY_RIGHT_DF, DataFrame(data={
        "C": [], "D": [], "E": [], "A": [], "B": [], "E_y": [], "G": []
    }).astype({"C": "int64", "D": "object", "E": "int64", "A": "int64", "B": "object", "E_y": "int64", "G": "object"})],
    [InnerJoinRule, ["A", "B"], None, (None, "_y"), EMPTY_LEFT_DF, EMPTY_RIGHT_DF, DataFrame(data={
        "C": [], "D": [], "E": [], "A": [], "B": [], "E_y": [], "G": []
    }).astype({"C": "int64", "D": "object", "E": "int64", "A": "int64", "B": "object", "E_y": "int64", "G": "object"})],
    [OuterJoinRule, ["A", "B"], None, (None, "_y"), EMPTY_LEFT_DF, EMPTY_RIGHT_DF, DataFrame(data={
        "C": [], "D": [], "E": [], "A": [], "B": [], "E_y": [], "G": []
    }).astype({"C": "int64", "D": "object", "E": "int64", "A": "int64", "B": "object", "E_y": "int64", "G": "object"})],
    [RightJoinRule, ["A", "B"], None, (None, "_y"), EMPTY_LEFT_DF, EMPTY_RIGHT_DF, DataFrame(data={
        "C": [], "D": [], "E": [], "A": [], "B": [], "E_y": [], "G": []
    }).astype({"C": "int64", "D": "object", "E": "int64", "A": "int64", "B": "object", "E_y": "int64", "G": "object"})],
])
def test_empty_df_join_scenarios(rule_cls, key_columns_left, key_columns_right, suffixes, left_df, right_df, expected):
    with get_test_data(left_df, named_inputs={"right": right_df}, named_output="result") as data:
        rule = rule_cls(named_input_left=None, named_input_right="right", key_columns_left=key_columns_left, 
                        key_columns_right=key_columns_right, suffixes=suffixes, named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


LEFT_DF_JOIN_TO_SELF_RESULT = DataFrame(data=[
    {"A": 1, "B": "b", "C_x": 10, "D_x": "test", "E_x": 3, "C_y": 10, "D_y": "test", "E_y": 3},
    {"A": 2, "B": "b", "C_x": 10, "D_x": "test", "E_x": 4, "C_y": 10, "D_y": "test", "E_y": 4},
    {"A": 3, "B": "b", "C_x": 10, "D_x": "test", "E_x": 5, "C_y": 10, "D_y": "test", "E_y": 5},
    {"A": 4, "B": "b", "C_x": 10, "D_x": "test", "E_x": 6, "C_y": 10, "D_y": "test", "E_y": 6},
])

@pytest.mark.parametrize("rule_cls,key_columns_left,key_columns_right,named_input_left,named_input_right,named_output,expected", [
    [LeftJoinRule, ["A", "B"], None, "input", "input", "result", LEFT_DF_JOIN_TO_SELF_RESULT],
    [LeftJoinRule, ["A", "B"], None, None, None, None, LEFT_DF_JOIN_TO_SELF_RESULT],
    [RightJoinRule, ["A", "B"], None, "input", "input", "result", LEFT_DF_JOIN_TO_SELF_RESULT],
    [RightJoinRule, ["A", "B"], None, None, None, None, LEFT_DF_JOIN_TO_SELF_RESULT],
    [InnerJoinRule, ["A", "B"], None, "input", "input", "result", LEFT_DF_JOIN_TO_SELF_RESULT],
    [InnerJoinRule, ["A", "B"], None, None, None, None, LEFT_DF_JOIN_TO_SELF_RESULT],
    [OuterJoinRule, ["A", "B"], None, "input", "input", "result", LEFT_DF_JOIN_TO_SELF_RESULT],
    [OuterJoinRule, ["A", "B"], None, None, None, None, LEFT_DF_JOIN_TO_SELF_RESULT],
])
def test_join_to_itself(rule_cls, key_columns_left, key_columns_right, named_input_left, named_input_right, named_output, expected):
    with get_test_data(LEFT_DF, named_inputs={"input": LEFT_DF}, named_output=named_output) as data:
        rule = rule_cls(named_input_left=named_input_left, named_input_right=named_input_right, key_columns_left=key_columns_left, 
                        key_columns_right=key_columns_right, suffixes=["_x", "_y"], named_output=named_output)
        rule.apply(data)
        result = data.get_named_output(named_output) if named_output is not None else data.get_main_output()
        assert_frame_equal(result, expected)


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
