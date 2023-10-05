from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.backends.pandas import TypeConversionRule
from etlrules.exceptions import MissingColumnError, UnsupportedTypeError
from tests.backends.pandas.utils.data import get_test_data


@pytest.mark.parametrize("input_df,conversion_dict,expected_df", [
    [DataFrame(data=[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ]), {'A': 'int64', 'B': 'str'}, DataFrame(data=[
        {'A': 1, 'B': '2'},
        {'A': 2, 'B': '3'},
        {'A': 3, 'B': '4'},
    ])],
    [DataFrame(data=[
        {'A': '1.5', 'B': 2.0},
        {'A': '2.0', 'B': 3.45},
        {'A': '3.45', 'B': 4.5},
    ]), {'A': 'float64', 'B': 'str'}, DataFrame(data=[
        {'A': 1.5, 'B': '2.0'},
        {'A': 2.0, 'B': '3.45'},
        {'A': 3.45, 'B': '4.5'},
    ])],
    [DataFrame(data=[
        {'A': 1, 'B': 2.0},
        {'A': 2, 'B': 3.0},
        {'A': 3, 'B': 4.0},
    ]), {'A': 'float64', 'B': 'int64'}, DataFrame(data=[
        {'A': 1.0, 'B': 2},
        {'A': 2.0, 'B': 3},
        {'A': 3.0, 'B': 4},
    ])],
])
def test_type_conversion_rule_scenarios(input_df, conversion_dict, expected_df):
    with get_test_data(input_df, named_inputs={"other": input_df}, named_output="result") as data:
        rule = TypeConversionRule(conversion_dict, named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected_df)


def test_type_conversion_rule_missing_column():
    df = DataFrame(data=[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ])
    with get_test_data(df, named_inputs={"other": df}, named_output="result") as data:
        rule = TypeConversionRule({
            'A': 'int64',
            'C': 'str',
        }, named_output="result")
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_type_conversion_rule_unsupported_type():
    df = DataFrame(data=[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ])
    with get_test_data(df, named_inputs={"other": df}, named_output="result") as data:
        rule = TypeConversionRule({
            'A': 'int64',
            'B': 'unknown_type',
        }, named_output="result")
        with pytest.raises(UnsupportedTypeError):
            rule.apply(data)
