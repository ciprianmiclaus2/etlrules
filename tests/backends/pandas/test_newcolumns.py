import numpy as np
from pandas import DataFrame, concat
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import ExpressionSyntaxError, ColumnAlreadyExistsError
from etlrules.backends.pandas import AddNewColumnRule
from tests.backends.pandas.utils.data import get_test_data


INPUT_DF = DataFrame(data=[
    {"A": 1, "B": 2, "C": 3, "D": "a", "E": "x", "F": 2, "G": "a"},
    {"A": 2, "B": 3, "C": 4, "D": "b", "E": "y"},
    {"A": 3, "B": 4, "C": 5, "D": "c", "E": "z"},
    {"A": 4, "B": 5, "C": 6, "D": "d", "E": "k", "F": 3, "G": "b"},
])


DF_OPS_SCENARIOS = [
    ["Sum", "df['A'] + df['B'] + df['C']", INPUT_DF, DataFrame(data=[
        {"Sum": 6}, {"Sum": 9}, {"Sum": 12}, {"Sum": 15}, 
    ]), None],
    ["SumEmpty", "df['A'] + df['B'] + df['C']", INPUT_DF[:0], DataFrame(data={"SumEmpty": []}, dtype="int64"), None],
    ["AddConst", "df['A'] + 10", INPUT_DF, DataFrame(data=[
        {"AddConst": 11}, {"AddConst": 12}, {"AddConst": 13}, {"AddConst": 14}, 
    ]), None],
    ["AddConstEmpty", "df['A'] + 10", INPUT_DF[:0], DataFrame(data={"AddConstEmpty": []}, dtype="int64"), None],
    ["Diff", "df['B'] - df['A']", INPUT_DF, DataFrame(data=[
        {"Diff": 1}, {"Diff": 1}, {"Diff": 1}, {"Diff": 1}, 
    ]), None],
    ["DiffEmpty", "df['B'] - df['A']", INPUT_DF[:0], DataFrame(data={"DiffEmpty": []}, dtype="int64"), None],
    ["Product", "df['A'] * df['B'] * df['C']", INPUT_DF, DataFrame(data=[
        {"Product": 6}, {"Product": 24}, {"Product": 60}, {"Product": 120}, 
    ]), None],
    ["ProductEmpty", "df['A'] * df['B'] * df['C']", INPUT_DF[:0], DataFrame(data={"ProductEmpty": []}, dtype="int64"), None],
    ["Div", "df['C'] / df['A']", INPUT_DF, DataFrame(data=[
        {"Div": 3.0}, {"Div": 2.0}, {"Div": 5/3}, {"Div": 6/4}, 
    ]), None],
    ["DivEmpty", "df['C'] / df['A']", INPUT_DF[:0], DataFrame(data={"DivEmpty": []}, dtype="float64"), None],
    ["Modulo", "df['B'] % df['A']", INPUT_DF, DataFrame(data=[
        {"Modulo": 0}, {"Modulo": 1}, {"Modulo": 1}, {"Modulo": 1}, 
    ]), None],
    ["ModuloEmpty", "df['B'] % df['A']", INPUT_DF[:0], DataFrame(data={"ModuloEmpty": []}, dtype="int64"), None],
    ["Pow", "df['A'] ** df['B']", INPUT_DF, DataFrame(data=[
        {"Pow": 1}, {"Pow": 8}, {"Pow": 81}, {"Pow": 1024}, 
    ]), None],
    ["PowEmpty", "df['A'] ** df['B']", INPUT_DF[:0], DataFrame(data={"PowEmpty": []}, dtype="int64"), None],
    ["BitwiseAND", "df['A'] & df['B']", INPUT_DF, DataFrame(data=[
        {"BitwiseAND": 1 & 2}, {"BitwiseAND": 2 & 3}, {"BitwiseAND": 3 & 4}, {"BitwiseAND": 4 & 5}, 
    ]), None],
    ["BitwiseANDEmpty", "df['A'] & df['B']", INPUT_DF[:0], DataFrame(data={"BitwiseANDEmpty": []}, dtype="int64"), None],
    ["BitwiseOR", "df['A'] | df['B']", INPUT_DF, DataFrame(data=[
        {"BitwiseOR": 1 | 2}, {"BitwiseOR": 2 | 3}, {"BitwiseOR": 3 | 4}, {"BitwiseOR": 4 | 5}, 
    ]), None],
    ["BitwiseOREmpty", "df['A'] | df['B']", INPUT_DF[:0], DataFrame(data={"BitwiseOREmpty": []}, dtype="int64"), None],
    ["BitwiseXOR", "df['A'] ^ df['B']", INPUT_DF, DataFrame(data=[
        {"BitwiseXOR": 1 ^ 2}, {"BitwiseXOR": 2 ^ 3}, {"BitwiseXOR": 3 ^ 4}, {"BitwiseXOR": 4 ^ 5}, 
    ]), None],
    ["BitwiseXOREmpty", "df['A'] ^ df['B']", INPUT_DF[:0], DataFrame(data={"BitwiseXOREmpty": []}, dtype="int64"), None],
    ["BitwiseComplement", "~df['A']", INPUT_DF, DataFrame(data=[
        {"BitwiseComplement": ~1}, {"BitwiseComplement": ~2}, {"BitwiseComplement": ~3}, {"BitwiseComplement": ~4}, 
    ]), None],
    ["BitwiseComplementEmpty", "~df['A']", INPUT_DF[:0], DataFrame(data={"BitwiseComplementEmpty": []}, dtype="int64"), None],

    ["StringConcat", "df['D'] + df['E']", INPUT_DF, DataFrame(data=[
        {"StringConcat": "ax"}, {"StringConcat": "by"}, {"StringConcat": "cz"}, {"StringConcat": "dk"}, 
    ]), None],
    ["StringConcatEmpty", "df['D'] + df['E']", INPUT_DF[:0], DataFrame(data={"StringConcatEmpty": []}, dtype="object"), None],
]


NON_DF_OPS_SCENARIOS = [
    ["BitwiseShiftRight", "df['B'] << df['A']", INPUT_DF, DataFrame(data=[
        {"BitwiseShiftRight": 2 << 1}, {"BitwiseShiftRight": 3 << 2}, {"BitwiseShiftRight": 4 << 3}, {"BitwiseShiftRight": 5 << 4}, 
    ]), None],
    ["BitwiseShiftRightEmpty", "df['B'] << df['A']", INPUT_DF[:0], DataFrame(data={"BitwiseShiftRightEmpty": []}, dtype="object"), None],
    ["BitwiseShiftLeft", "df['A'] >> df['B']", INPUT_DF, DataFrame(data=[
        {"BitwiseShiftLeft": 1 >> 2}, {"BitwiseShiftLeft": 2 >> 3}, {"BitwiseShiftLeft": 3 >> 4}, {"BitwiseShiftLeft": 4 >> 5}, 
    ]), None],
    ["BitwiseShiftLeftEmpty", "df['A'] >> df['B']", INPUT_DF[:0], DataFrame(data={"BitwiseShiftLeftEmpty": []}, dtype="object"), None],
]


NA_OPS_SCENARIOS = [
    ["Sum", "df['A'] + df['F']", INPUT_DF, DataFrame(data=[
        {"Sum": 3.0}, {"Sum": np.nan}, {"Sum": np.nan}, {"Sum": 7.0}, 
    ]), None],
    ["SumEmpty", "df['A'] + df['F']", INPUT_DF[:0], DataFrame(data={"SumEmpty": []}, dtype="float64"), None],
    ["StringConcat", "df['D'] + df['G']", INPUT_DF, DataFrame(data=[
        {"StringConcat": "aa"}, {"StringConcat": np.nan}, {"StringConcat": np.nan}, {"StringConcat": "db"}, 
    ]), None],
    ["StringConcatEmpty", "df['D'] + df['G']", INPUT_DF[:0], DataFrame(data={"StringConcatEmpty": []}, dtype="object"), None],
]


ERROR_SCENARIOS = [
    ["A", "df['B'] + df['C']", INPUT_DF, ColumnAlreadyExistsError, "Column A already exists in the input dataframe"],
    ["A", "df['B'] + df['C']", INPUT_DF[:0], ColumnAlreadyExistsError, "Column A already exists in the input dataframe"],
    ["ERR", "df['B'", INPUT_DF, ExpressionSyntaxError, "Error in expression 'df['B'':"],
    ["ERR", "df['B'", INPUT_DF[:0], ExpressionSyntaxError, "Error in expression 'df['B'':"],
    ["ERR", "df['UNKNOWN'] + 1", INPUT_DF, KeyError, "UNKNOWN"],
    ["ERR", "df['UNKNOWN'] + 1", INPUT_DF[:0], KeyError, "UNKNOWN"],
    ["ERR", "df['A'] + unknown", INPUT_DF, NameError, "name 'unknown' is not defined"],
    ["ERR", "df['A'] + unknown", INPUT_DF[:0], NameError, "name 'unknown' is not defined"],
    ["ERR", "for i in df['A']:print(i)", INPUT_DF, ExpressionSyntaxError, "Error in expression 'for i in df['A']:print(i)': invalid syntax"],  # only expressions allowed
    ["ERR", "for i in df['A']:print(i)", INPUT_DF[:0], ExpressionSyntaxError, "Error in expression 'for i in df['A']:print(i)': invalid syntax"],  # only expressions allowed
    ["IntStringConcat", "df['A'] + df['D']", INPUT_DF, TypeError, "unsupported operand type(s) for +: 'int' and 'str'"],
    ["IntStringConcat", "df['A'] + df['D']", INPUT_DF[:0], DataFrame(data={"IntStringConcat": []}, dtype="object"), None],
]


@pytest.mark.parametrize("column_name,expression,input_df,expected,expected_info",
    DF_OPS_SCENARIOS +
    NON_DF_OPS_SCENARIOS +
    NA_OPS_SCENARIOS +
    ERROR_SCENARIOS
)
def test_add_new_column(column_name, expression, input_df, expected, expected_info):
    with get_test_data(input_df, named_inputs={"copy": input_df}, named_output="result") as data:
        if isinstance(expected, DataFrame):
            rule = AddNewColumnRule(column_name, expression, named_input="copy", named_output="result")
            rule.apply(data)
            result = data.get_named_output("result")
            expected = concat((input_df, expected), axis=1)
            assert_frame_equal(result, expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected) as exc:
                rule = AddNewColumnRule(column_name, expression, named_input="copy", named_output="result")
                rule.apply(data)
            assert expected_info in str(exc.value)
        else:
            assert False, f"Unexpected {type(expected)} in '{expected}'"
