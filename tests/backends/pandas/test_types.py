from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest
from finrules.backends.pandas import TypeConversionRule
from finrules.data import RuleData
from finrules.exceptions import MissingColumn, UnsupportedType


def test_type_conversion_rule_simple():
    df = DataFrame(data=[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ])
    data = RuleData(df)
    rule = TypeConversionRule({
        'A': 'int64',
        'B': 'str',
    })
    rule.apply(data)
    expected = DataFrame(data=[
        {'A': 1, 'B': '2'},
        {'A': 2, 'B': '3'},
        {'A': 3, 'B': '4'},
    ])
    assert_frame_equal(data.get_main_output(), expected)


def test_type_conversion_rule_missing_column():
    df = DataFrame(data=[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ])
    data = RuleData(df)
    rule = TypeConversionRule({
        'A': 'int64',
        'C': 'str',
    })
    with pytest.raises(MissingColumn):
        rule.apply(data)


def test_type_conversion_rule_unsupported_type():
    df = DataFrame(data=[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ])
    data = RuleData(df)
    rule = TypeConversionRule({
        'A': 'int64',
        'B': 'unknown_type',
    })
    with pytest.raises(UnsupportedType):
        rule.apply(data)
