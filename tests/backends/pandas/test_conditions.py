from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest
from etlrules.backends.pandas import IfThenElseRule, FilterRule
from etlrules.exceptions import ColumnAlreadyExistsError, ExpressionSyntaxError, MissingColumnError
from tests.backends.pandas.utils.data import get_test_data


INPUT_DF = DataFrame(data=[
    {"A": 1, "B": 2, "C": 3, "D": 4},
    {"A": 5, "B": 3, "C": 1, "D": 9},
    {"A": 3, "B": 4, "C": 2, "D": 1},
    {"A": 3, "C": 2, "D": 1},
    {"B": 4, "C": 2, "D": 1},
])


@pytest.mark.parametrize("condition_expression,output_column,then_value,then_column,else_value,else_column,input_df,expected", [
    ["df['A'] > df['B']", "O", "A is greater", None, "B is greater", None, INPUT_DF, DataFrame(data=[
        {"A": 1, "B": 2, "C": 3, "D": 4, "O": "B is greater"},
        {"A": 5, "B": 3, "C": 1, "D": 9, "O": "A is greater"},
        {"A": 3, "B": 4, "C": 2, "D": 1, "O": "B is greater"},
        {"A": 3, "C": 2, "D": 1, "O": "B is greater"},
        {"B": 4, "C": 2, "D": 1, "O": "B is greater"},
    ])],
    ["df['A'] > df['B']", "O", None, "C", None, "D", INPUT_DF, DataFrame(data=[
        {"A": 1, "B": 2, "C": 3, "D": 4, "O": 4},
        {"A": 5, "B": 3, "C": 1, "D": 9, "O": 1},
        {"A": 3, "B": 4, "C": 2, "D": 1, "O": 1},
        {"A": 3, "C": 2, "D": 1, "O": 1},
        {"B": 4, "C": 2, "D": 1, "O": 1},
    ])],
    ["df['A'] > df['B']", "O", "A is greater", None, "B is greater", None, DataFrame(data={"A": [], "B": []}, dtype="Int64"), DataFrame(data={"A": [], "B": [], "O": []}).astype({"A": "Int64", "B": "Int64", "O": "object"})],
    ["df['A'] > df['B']", "O", None, "C", None, "E", INPUT_DF, MissingColumnError],
    ["df['A'] > df['B']", "O", None, "E", None, "D", INPUT_DF, MissingColumnError],
    ["df['A'] > df['B']", "B", None, "C", None, "D", INPUT_DF, ColumnAlreadyExistsError],
    ["df['A' > df['B']", "O", None, "C", None, "D", INPUT_DF, ExpressionSyntaxError],
    ["df['A'] > df['UNKNOWN']", "O", None, "C", None, "D", INPUT_DF, KeyError],
])
def test_if_then_else_scenarios(condition_expression, output_column, then_value, then_column, else_value, else_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, DataFrame):
            rule = IfThenElseRule(
                condition_expression=condition_expression, output_column=output_column, then_value=then_value,
                then_column=then_column, else_value=else_value, else_column=else_column, named_input="input", named_output="result")
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule = IfThenElseRule(
                    condition_expression=condition_expression, output_column=output_column, then_value=then_value,
                    then_column=then_column, else_value=else_value, else_column=else_column, named_input="input", named_output="result")
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("condition_expression,discard_matching_rows,named_output_discarded,input_df,expected,discarded_expected", [
    ["df['A'] > df['B']", False, "discarded", DataFrame(data=[
        {"A": 1, "B": 2}, {"A": 5, "B": 3}, {"A": 3, "B": 4},
    ]), DataFrame(data=[
        {"A": 5, "B": 3}
    ]), DataFrame(data=[
        {"A": 1, "B": 2}, {"A": 3, "B": 4},
    ])],
    ["df['A'] > df['B']", True, "discarded", DataFrame(data=[
        {"A": 1, "B": 2}, {"A": 5, "B": 3}, {"A": 3, "B": 4},
    ]), DataFrame(data=[
        {"A": 1, "B": 2}, {"A": 3, "B": 4},
    ]), DataFrame(data=[
        {"A": 5, "B": 3}
    ])],
    ["df['A'] > df['B']", True, "discarded", DataFrame(data={"A": [], "B": []}, dtype="Int64"),
        DataFrame(data={"A": [], "B": []}, dtype="Int64"),
        DataFrame(data={"A": [], "B": []}, dtype="Int64"),
    ],
    ["df['A' > df['B']", False, None, INPUT_DF, ExpressionSyntaxError, None],
    ["df['A'] > df['UNKNOWN']", False, None, INPUT_DF, KeyError, None],
])
def test_filter_rule_scenarios(condition_expression, discard_matching_rows, named_output_discarded, input_df, expected, discarded_expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, DataFrame):
            rule = FilterRule(
                condition_expression=condition_expression, discard_matching_rows=discard_matching_rows,
                named_output_discarded=named_output_discarded, named_input="input", named_output="result")
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
            if named_output_discarded is not None:
                assert_frame_equal(data.get_named_output(named_output_discarded), discarded_expected)
            else:
                assert discarded_expected is None
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule = FilterRule(
                    condition_expression=condition_expression, discard_matching_rows=discard_matching_rows,
                    named_output_discarded=named_output_discarded, named_input="input", named_output="result")
                rule.apply(data)
        else:
            assert False