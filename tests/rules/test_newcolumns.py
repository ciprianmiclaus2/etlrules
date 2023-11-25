import datetime
import pytest

from etlrules.data import context
from etlrules.exceptions import ExpressionSyntaxError, ColumnAlreadyExistsError, UnsupportedTypeError
from tests.utils.data import assert_frame_equal, get_test_data


INPUT_DF = [
    {"A": 1, "B": 2, "C": 3, "D": "a", "E": "x", "F": 2, "G": "a", "H": datetime.datetime(2023, 11, 5, 10, 30)},
    {"A": 2, "B": 3, "C": 4, "D": "b", "E": "y"},
    {"A": 3, "B": 4, "C": 5, "D": "c", "E": "z"},
    {"A": 4, "B": 5, "C": 6, "D": "d", "E": "k", "F": 3, "G": "b", "H": datetime.datetime(2023, 11, 5, 11, 30)},
]
INPUT_DF_TYPES = {
    "A": "Int64", "B": "Int64", "C": "Int64", "D": "string", "E": "string", "F": "Int64", "G": "string", "H": "datetime"
}


DF_OPS_SCENARIOS = [
    ["Sum", "df['A'] + df['B'] + df['C']", None, INPUT_DF, [
        {"Sum": 6}, {"Sum": 9}, {"Sum": 12}, {"Sum": 15}, 
    ], "Int64", None],
    ["SumEmpty", "df['A'] + df['B'] + df['C']", None, None, {"SumEmpty": []}, "Int64", None],
    ["AddConst", "df['A'] + 10", None, INPUT_DF, [
        {"AddConst": 11}, {"AddConst": 12}, {"AddConst": 13}, {"AddConst": 14}, 
    ], "Int64", None],
    ["AddConstEmpty", "df['A'] + 10", None, None, {"AddConstEmpty": []}, "Int64", None],
    ["Diff", "df['B'] - df['A']", None, INPUT_DF, [
        {"Diff": 1}, {"Diff": 1}, {"Diff": 1}, {"Diff": 1}, 
    ], "Int64", None],
    ["DiffEmpty", "df['B'] - df['A']", None, None, {"DiffEmpty": []}, "Int64", None],
    ["Product", "df['A'] * df['B'] * df['C']", None, INPUT_DF, [
        {"Product": 6}, {"Product": 24}, {"Product": 60}, {"Product": 120}, 
    ], "Int64", None],
    ["ProductEmpty", "df['A'] * df['B'] * df['C']", None, None, {"ProductEmpty": []}, "Int64", None],
    ["Div", "df['C'] / df['A']", None, INPUT_DF, [
        {"Div": 3.0}, {"Div": 2.0}, {"Div": 5/3}, {"Div": 6/4}, 
    ], "Float64", None],
    ["DivEmpty", "df['C'] / df['A']", None, None, {"DivEmpty": []}, "Float64", None],
    ["Modulo", "df['B'] % df['A']", None, INPUT_DF, [
        {"Modulo": 0}, {"Modulo": 1}, {"Modulo": 1}, {"Modulo": 1}, 
    ], "Int64", None],
    ["ModuloEmpty", "df['B'] % df['A']", None, None, {"ModuloEmpty": []}, "Int64", None],
    ["Pow", "df['A'] ** df['B']", "int64", INPUT_DF, [
        {"Pow": 1}, {"Pow": 8}, {"Pow": 81}, {"Pow": 1024}, 
    ], "Int64", None],
    ["PowEmpty", "df['A'] ** df['B']", "int64", None, {"PowEmpty": []}, "Int64", None],
    ["BitwiseAND", "df['A'] & df['B']", None, INPUT_DF, [
        {"BitwiseAND": 1 & 2}, {"BitwiseAND": 2 & 3}, {"BitwiseAND": 3 & 4}, {"BitwiseAND": 4 & 5}, 
    ], "Int64", None],
    ["BitwiseANDEmpty", "df['A'] & df['B']", None, None, {"BitwiseANDEmpty": []}, "Int64", None],
    ["BitwiseOR", "df['A'] | df['B']", None, INPUT_DF, [
        {"BitwiseOR": 1 | 2}, {"BitwiseOR": 2 | 3}, {"BitwiseOR": 3 | 4}, {"BitwiseOR": 4 | 5}, 
    ], "Int64", None],
    ["BitwiseOREmpty", "df['A'] | df['B']", None, None, {"BitwiseOREmpty": []}, "Int64", None],
    ["BitwiseXOR", "df['A'] ^ df['B']", None, INPUT_DF, [
        {"BitwiseXOR": 1 ^ 2}, {"BitwiseXOR": 2 ^ 3}, {"BitwiseXOR": 3 ^ 4}, {"BitwiseXOR": 4 ^ 5}, 
    ], "Int64", None],
    ["BitwiseXOREmpty", "df['A'] ^ df['B']", None, None, {"BitwiseXOREmpty": []}, "Int64", None],
    ["BitwiseComplement", "~df['A']", None, INPUT_DF, [
        {"BitwiseComplement": ~1}, {"BitwiseComplement": ~2}, {"BitwiseComplement": ~3}, {"BitwiseComplement": ~4}, 
    ], "Int64", None],
    ["BitwiseComplementEmpty", "~df['A']", "int64", None, {"BitwiseComplementEmpty": []}, "Int64", None],

    ["StringConcat", "df['D'] + df['E']", None, INPUT_DF, [
        {"StringConcat": "ax"}, {"StringConcat": "by"}, {"StringConcat": "cz"}, {"StringConcat": "dk"}, 
    ], "string", None],
    ["StringConcatEmpty", "df['D'] + df['E']", None, None, {"StringConcatEmpty": []}, "string", None],
]


DF_CONTEXT_SCENARIOS = [
    ["Cnst", "df['A'] + context.int_val", None, INPUT_DF, [
        {"Cnst": 3}, {"Cnst": 4}, {"Cnst": 5}, {"Cnst": 6}, 
    ], "Int64", None],
    ["Cnst", "df['D'] + context.str_val", None, INPUT_DF, [
        {"Cnst": "aSTR1"}, {"Cnst": "bSTR1"}, {"Cnst": "cSTR1"}, {"Cnst": "dSTR1"}, 
    ], "string", None],
    ["Cnst", "df['A'] and context.bool_val", "boolean", INPUT_DF, [
        {"Cnst": True}, {"Cnst": True}, {"Cnst": True}, {"Cnst": True}, 
    ], "boolean", None],
]


NON_DF_OPS_SCENARIOS = [
    ["BitwiseShiftRight", "df['B'] << df['A']", None, INPUT_DF, [
        {"BitwiseShiftRight": 2 << 1}, {"BitwiseShiftRight": 3 << 2}, {"BitwiseShiftRight": 4 << 3}, {"BitwiseShiftRight": 5 << 4}, 
    ], None, None],
    ["BitwiseShiftRightEmpty", "df['B'] << df['A']", None, None, {"BitwiseShiftRightEmpty": []}, "string", None],
    ["BitwiseShiftLeft", "df['A'] >> df['B']", None, INPUT_DF, [
        {"BitwiseShiftLeft": 1 >> 2}, {"BitwiseShiftLeft": 2 >> 3}, {"BitwiseShiftLeft": 3 >> 4}, {"BitwiseShiftLeft": 4 >> 5}, 
    ], None, None],
    ["BitwiseShiftLeftEmpty", "df['A'] >> df['B']", None, None, {"BitwiseShiftLeftEmpty": []}, "string", None],
]


NA_OPS_SCENARIOS = [
    ["Sum", "df['A'] + df['F']", None, INPUT_DF, [
        {"Sum": 3}, {}, {}, {"Sum": 7}, 
    ], "Int64", None],
    ["SumEmpty", "df['A'] + df['F']", None, None, {"SumEmpty": []}, "Int64", None],
    ["StringConcat", "df['D'] + df['G']", None, INPUT_DF, [
        {"StringConcat": "aa"}, {"StringConcat": None}, {"StringConcat": None}, {"StringConcat": "db"}, 
    ], "string", None],
    ["StringConcatEmpty", "df['D'] + df['G']", None, None, {"StringConcatEmpty": []}, "string", None],
]


ERROR_SCENARIOS = [
    ["A", "df['B'] + df['C']", None, INPUT_DF, ColumnAlreadyExistsError, None, "Column A already exists in the input dataframe"],
    ["A", "df['B'] + df['C']", None, None, ColumnAlreadyExistsError, None, "Column A already exists in the input dataframe"],
    ["ERR", "df['B'", None, INPUT_DF, ExpressionSyntaxError, None, "Error in expression 'df['B'':"],
    ["ERR", "df['B'", None, None, ExpressionSyntaxError, None, "Error in expression 'df['B'':"],
    ["ERR", "df['UNKNOWN'] + 1", None, INPUT_DF, KeyError, None, "UNKNOWN"],
    ["ERR", "df['UNKNOWN'] + 1", None, None, KeyError,None,  "UNKNOWN"],
    ["ERR", "df['A'] + unknown", None, INPUT_DF, NameError, None, "name 'unknown' is not defined"],
    ["ERR", "df['A'] + unknown", None, None, NameError, None, "name 'unknown' is not defined"],
    ["ERR", "for i in df['A']:print(i)", None, INPUT_DF, ExpressionSyntaxError, None, "Error in expression 'for i in df['A']:print(i)': invalid syntax"],  # only expressions allowed
    ["ERR", "for i in df['A']:print(i)", None, None, ExpressionSyntaxError, None, "Error in expression 'for i in df['A']:print(i)': invalid syntax"],  # only expressions allowed
    ["IntStringConcat", "df['A'] + df['D']", None, None, {"IntStringConcat": []}, "string", None],
    ["UnsupportedType", "df['D'] + df['E']", "unknown", INPUT_DF, UnsupportedTypeError, None, None],
]


@pytest.mark.parametrize("column_name,expression,expression_type,input_df_in,expected,expected_dtype,expected_info",
    DF_OPS_SCENARIOS +
    NON_DF_OPS_SCENARIOS +
    NA_OPS_SCENARIOS +
    DF_CONTEXT_SCENARIOS +
    ERROR_SCENARIOS
)
def test_add_new_column(column_name, expression, expression_type, input_df_in, expected, expected_dtype, expected_info, backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_DF_TYPES)
    if input_df_in is None:
        input_df = input_df[:0]
    expected = backend.DataFrame(expected, dtype=expected_dtype) if isinstance(expected, (list, dict)) else expected
    with context.set({"str_val": "STR1", "int_val": 2, "float_val": 3.5, "bool_val": True}):
        with get_test_data(input_df, named_inputs={"copy": input_df}, named_output="result") as data:
            if isinstance(expected, backend.impl.DataFrame):
                rule = backend.rules.AddNewColumnRule(column_name, expression, expression_type, named_input="copy", named_output="result")
                rule.apply(data)
                result = data.get_named_output("result")
                expected = backend.hconcat(input_df, expected)
                assert_frame_equal(result, expected)
            elif issubclass(expected, Exception):
                with pytest.raises(expected) as exc:
                    rule = backend.rules.AddNewColumnRule(column_name, expression, expression_type, named_input="copy", named_output="result")
                    rule.apply(data)
                if expected_info:
                    assert expected_info in str(exc.value)
            else:
                assert False, f"Unexpected {type(expected)} in '{expected}'"


INPUT_DF2 = [
    {"A": 10, "B": "b"},
    {"A": 5, "B": "a"},
    {"B": "c"},
    {"A": 6},
    {}
]


@pytest.mark.parametrize("output_column,start,step,strict,input_df_in,expected,expected_info", [
    ["C", 0, 1, True, INPUT_DF2, [
        {"A": 10, "B": "b", "C": 0},
        {"A": 5, "B": "a", "C": 1},
        {"B": "c", "C": 2},
        {"A": 6, "C": 3},
        {"C": 4}
    ], None],
    ["C", 8, 1, True, INPUT_DF2, [
        {"A": 10, "B": "b", "C": 8},
        {"A": 5, "B": "a", "C": 9},
        {"B": "c", "C": 10},
        {"A": 6, "C": 11},
        {"C": 12}
    ], None],
    ["C", 10, 2, True, INPUT_DF2, [
        {"A": 10, "B": "b", "C": 10},
        {"A": 5, "B": "a", "C": 12},
        {"B": "c", "C": 14},
        {"A": 6, "C": 16},
        {"C": 18}
    ], None],
    ["C", 10, 2, True, None, {"A": [], "B": [], "C": []}, None],
    ["B", 10, 2, True, None, ColumnAlreadyExistsError, "Column B already exists in the input dataframe."],
    ["B", 10, 2, False, INPUT_DF2, [
        {"A": 10, "B": 10},
        {"A": 5, "B": 12},
        {"B": 14},
        {"A": 6, "B": 16},
        {"B": 18}
    ], {"A": "Int64", "B": "int64"}],
])
def test_add_row_numbers(output_column, start, step, strict, input_df_in, expected, expected_info, backend):
    input_df = backend.DataFrame(INPUT_DF2, astype={"A": "Int64", "B": "string"})
    if input_df_in is None:
        input_df = input_df[:0]
    if isinstance(expected, (list, dict)):
        expected = backend.DataFrame(expected, astype=expected_info or {"A": "Int64", "B": "string", "C": "int64"})
    with get_test_data(input_df, named_inputs={"copy": input_df}, named_output="result") as data:
        if isinstance(expected, backend.impl.DataFrame):
            rule = backend.rules.AddRowNumbersRule(output_column, start, step, named_input="copy", named_output="result", strict=strict)
            rule.apply(data)
            result = data.get_named_output("result")
            assert_frame_equal(result, expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected) as exc:
                rule = backend.rules.AddRowNumbersRule(output_column, start, step, named_input="copy", named_output="result", strict=strict)
                rule.apply(data)
            if expected_info:
                assert expected_info in str(exc.value)
        else:
            assert False, f"Unexpected {type(expected)} in '{expected}'"
