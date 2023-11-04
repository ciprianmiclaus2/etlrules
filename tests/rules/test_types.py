import pytest

from etlrules.exceptions import MissingColumnError, UnsupportedTypeError
from tests.utils.data import assert_frame_equal, get_test_data


@pytest.mark.parametrize("input_df,input_astype,conversion_dict,expected_df,expected_astype", [
    [[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ], None, {'A': 'int64', 'B': 'string'}, [
        {'A': 1, 'B': '2'},
        {'A': 2, 'B': '3'},
        {'A': 3, 'B': '4'},
    ], {"A": "Int64", "B": "string"}],
    [[
        {'A': '1.5', 'B': 2.0},
        {'A': '2.0', 'B': 3.45},
        {'A': '3.45', 'B': 4.5},
    ], None, {'A': 'float64', 'B': 'string'}, [
        {'A': 1.5, 'B': '2.0'},
        {'A': 2.0, 'B': '3.45'},
        {'A': 3.45, 'B': '4.5'},
    ], {"B": "string"}],
    [[
        {'A': 1, 'B': 2.0},
        {'A': 2, 'B': 3.0},
        {'A': 3, 'B': 4.0},
    ], None, {'A': 'float64', 'B': 'int64'}, [
        {'A': 1.0, 'B': 2},
        {'A': 2.0, 'B': 3},
        {'A': 3.0, 'B': 4},
    ], {"B": "Int64"}],
    [{"A": [], "B": []}, {"A": "string", "B": "int64"},
        {'A': 'int64', 'B': 'string'},
        {"A": [], "B": []}, {"A": "Int64", "B": "string"}],
])
def test_type_conversion_rule_scenarios(input_df, input_astype, conversion_dict, expected_df, expected_astype, backend):
    input_df = backend.DataFrame(input_df, astype=input_astype)
    expected_df = backend.DataFrame(expected_df, astype=expected_astype)
    with get_test_data(input_df, named_inputs={"other": input_df}, named_output="result") as data:
        rule = backend.rules.TypeConversionRule(conversion_dict, named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected_df)


def test_type_conversion_rule_missing_column(backend):
    df = backend.DataFrame([
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ])
    with get_test_data(df, named_inputs={"other": df}, named_output="result") as data:
        rule = backend.rules.TypeConversionRule({
            'A': 'int64',
            'C': 'string',
        }, named_output="result")
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_type_conversion_rule_unsupported_type(backend):
    df = backend.DataFrame([
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ])
    with get_test_data(df, named_inputs={"other": df}, named_output="result") as data:
        rule = backend.rules.TypeConversionRule({
            'A': 'int64',
            'B': 'unknown_type',
        }, named_output="result")
        with pytest.raises(UnsupportedTypeError):
            rule.apply(data)
