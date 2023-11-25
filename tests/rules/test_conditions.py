import pytest

from etlrules.data import context
from etlrules.exceptions import ColumnAlreadyExistsError, ExpressionSyntaxError, MissingColumnError
from tests.utils.data import assert_frame_equal, get_test_data


INPUT_DF = [
    {"A": 1, "B": 2, "C": 3, "D": 4},
    {"A": 5, "B": 3, "C": 1, "D": 9},
    {"A": 3, "B": 4, "C": 2, "D": 1},
    {"A": 3, "C": 2, "D": 1},
    {"B": 4, "C": 2, "D": 1},
]


@pytest.mark.parametrize("condition_expression,output_column,then_value,then_column,else_value,else_column,input_df,input_astype,expected,expected_astype", [
    ["df['A'] > df['B']", "O", "A is greater", None, "B is greater", None, INPUT_DF, None, [
        {"A": 1, "B": 2, "C": 3, "D": 4, "O": "B is greater"},
        {"A": 5, "B": 3, "C": 1, "D": 9, "O": "A is greater"},
        {"A": 3, "B": 4, "C": 2, "D": 1, "O": "B is greater"},
        {"A": 3, "C": 2, "D": 1, "O": "B is greater"},
        {"B": 4, "C": 2, "D": 1, "O": "B is greater"},
    ], None],
    ["df['A'] > df['B']", "O", None, "C", None, "D", INPUT_DF, None, [
        {"A": 1, "B": 2, "C": 3, "D": 4, "O": 4},
        {"A": 5, "B": 3, "C": 1, "D": 9, "O": 1},
        {"A": 3, "B": 4, "C": 2, "D": 1, "O": 1},
        {"A": 3, "C": 2, "D": 1, "O": 1},
        {"B": 4, "C": 2, "D": 1, "O": 1},
    ], None],
    ["df['A'] > context.int_val", "O", "A is greater", None, "A is smaller", None, INPUT_DF, None, [
        {"A": 1, "B": 2, "C": 3, "D": 4, "O": "A is smaller"},
        {"A": 5, "B": 3, "C": 1, "D": 9, "O": "A is greater"},
        {"A": 3, "B": 4, "C": 2, "D": 1, "O": "A is greater"},
        {"A": 3, "C": 2, "D": 1, "O": "A is greater"},
        {"B": 4, "C": 2, "D": 1, "O": "A is smaller"},
    ], None],
    ["df['A'] > df['B']", "O", "A is greater", None, "B is greater", None, {"A": [], "B": []}, {"A": "Int64", "B": "Int64"}, {"A": [], "B": [], "O": []}, {"A": "Int64", "B": "Int64", "O": "string"}],
    ["df['A'] > df['B']", "O", None, "C", None, "E", INPUT_DF, None, MissingColumnError, None],
    ["df['A'] > df['B']", "O", None, "E", None, "D", INPUT_DF, None, MissingColumnError, None],
    ["df['A'] > df['B']", "B", None, "C", None, "D", INPUT_DF, None, ColumnAlreadyExistsError, None],
    ["df['A' > df['B']", "O", None, "C", None, "D", INPUT_DF, None, ExpressionSyntaxError, None],
    ["df['A'] > df['UNKNOWN']", "O", None, "C", None, "D", INPUT_DF, None, KeyError, None],
])
def test_if_then_else_scenarios(condition_expression, output_column, then_value, then_column, else_value, else_column, input_df, input_astype, expected, expected_astype, backend):
    input_df = backend.DataFrame(input_df, astype=input_astype)
    expected = backend.DataFrame(expected, astype=expected_astype) if isinstance(expected, (list, dict)) else expected
    with context.set({"str_val": "STR1", "int_val": 2, "float_val": 3.5, "bool_val": True}):
        with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
            if isinstance(expected, backend.impl.DataFrame):
                rule = backend.rules.IfThenElseRule(
                    condition_expression=condition_expression, output_column=output_column, then_value=then_value,
                    then_column=then_column, else_value=else_value, else_column=else_column, named_input="input", named_output="result")
                rule.apply(data)
                actual = data.get_named_output("result")
                assert_frame_equal(actual, expected)
            elif issubclass(expected, Exception):
                with pytest.raises(expected):
                    rule = backend.rules.IfThenElseRule(
                        condition_expression=condition_expression, output_column=output_column, then_value=then_value,
                        then_column=then_column, else_value=else_value, else_column=else_column, named_input="input", named_output="result")
                    rule.apply(data)
            else:
                assert False


@pytest.mark.parametrize("condition_expression,discard_matching_rows,named_output_discarded,input_df,input_dtype,expected,expected_dtype,discarded_expected,discarded_dtype", [
    ["df['A'] > df['B']", False, "discarded", [
        {"A": 1, "B": 2}, {"A": 5, "B": 3}, {"A": 3, "B": 4},
    ], None, [
        {"A": 5, "B": 3}
    ], None, [
        {"A": 1, "B": 2}, {"A": 3, "B": 4},
    ], None],
    ["df['A'] > context.int_val", False, "discarded", [
        {"A": 1, "B": 2}, {"A": 5, "B": 3}, {"A": 3, "B": 4},
    ], None, [
        {"A": 5, "B": 3}, {"A": 3, "B": 4}
    ], None, [
        {"A": 1, "B": 2},
    ], None],
    ["df['A'] > df['B']", True, "discarded", [
        {"A": 1, "B": 2}, {"A": 5, "B": 3}, {"A": 3, "B": 4},
    ], None, [
        {"A": 1, "B": 2}, {"A": 3, "B": 4},
    ], None, [
        {"A": 5, "B": 3}
    ], None],
    ["df['A'] > df['B']", True, "discarded", {"A": [], "B": []}, "Int64",
        {"A": [], "B": []}, "Int64",
        {"A": [], "B": []}, "Int64",
    ],
    ["df['A' > df['B']", False, None, INPUT_DF, None, ExpressionSyntaxError, None, None, None],
    ["df['A'] > df['UNKNOWN']", False, None, INPUT_DF, None, KeyError, None, None, None],
])
def test_filter_rule_scenarios(condition_expression, discard_matching_rows, named_output_discarded, input_df, input_dtype, expected, expected_dtype, discarded_expected, discarded_dtype, backend):
    input_df = backend.DataFrame(input_df, dtype=input_dtype)
    expected = backend.DataFrame(expected, dtype=expected_dtype) if isinstance(expected, (list, dict)) else expected
    discarded_expected = backend.DataFrame(discarded_expected, dtype=discarded_dtype) if isinstance(discarded_expected, (list, dict)) else discarded_expected
    with context.set({"str_val": "STR1", "int_val": 2, "float_val": 3.5, "bool_val": True}):
        with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
            if isinstance(expected, backend.impl.DataFrame):
                rule = backend.rules.FilterRule(
                    condition_expression=condition_expression, discard_matching_rows=discard_matching_rows,
                    named_output_discarded=named_output_discarded, named_input="input", named_output="result")
                rule.apply(data)
                actual = data.get_named_output("result")
                assert_frame_equal(actual, expected)
                if named_output_discarded is not None:
                    discarded_actual = data.get_named_output(named_output_discarded)
                    assert_frame_equal(discarded_actual, discarded_expected)
                else:
                    assert discarded_expected is None
            elif issubclass(expected, Exception):
                with pytest.raises(expected):
                    rule = backend.rules.FilterRule(
                        condition_expression=condition_expression, discard_matching_rows=discard_matching_rows,
                        named_output_discarded=named_output_discarded, named_input="input", named_output="result")
                    rule.apply(data)
            else:
                assert False