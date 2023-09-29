from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas import StrLowerRule, StrUpperRule, StrCapitalizeRule
from tests.backends.pandas.utils.data import get_test_data


INPUT_DF = DataFrame(data=[
    {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100},
    {"A": "babA", "B": -1.677, "C": "dDdd"},
    {"A": "cAAA", "B": 3.87, "D": -499},
    {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1},
])


@pytest.mark.parametrize("rule_cls,columns,input_df,expected", [
    [StrLowerRule, ["A", "C"], INPUT_DF, DataFrame(data=[
        {"A": "abcdefg", "B": 1.456, "C": "cccc", "D": -100},
        {"A": "baba", "B": -1.677, "C": "dddd"},
        {"A": "caaa", "B": 3.87, "D": -499},
        {"A": "diii", "B": -1.5, "C": "eeee", "D": 1},
    ])],
    [StrLowerRule, ["A", "C", "Z"], INPUT_DF, MissingColumnError],
    [StrUpperRule, ["A", "C"], INPUT_DF, DataFrame(data=[
        {"A": "ABCDEFG", "B": 1.456, "C": "CCCC", "D": -100},
        {"A": "BABA", "B": -1.677, "C": "DDDD"},
        {"A": "CAAA", "B": 3.87, "D": -499},
        {"A": "DIII", "B": -1.5, "C": "EEEE", "D": 1},
    ])],
    [StrUpperRule, ["A", "C", "Z"], INPUT_DF, MissingColumnError],
    [StrCapitalizeRule, ["A", "C"], INPUT_DF, DataFrame(data=[
        {"A": "Abcdefg", "B": 1.456, "C": "Cccc", "D": -100},
        {"A": "Baba", "B": -1.677, "C": "Dddd"},
        {"A": "Caaa", "B": 3.87, "D": -499},
        {"A": "Diii", "B": -1.5, "C": "Eeee", "D": 1},
    ])],
    [StrCapitalizeRule, ["A", "C", "Z"], INPUT_DF, MissingColumnError],
])
def test_str_scenarios(rule_cls, columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False
