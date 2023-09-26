from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas import RoundRule
from tests.backends.pandas.utils.data import get_test_data

INPUT_DF = DataFrame(data=[
    {"A": 1.456, "B": 1.456, "C": 3.8734},
    {"A": 1.455, "B": 1.677, "C": 3.8739},
    {"A": 1.4, "C": 3.87},
    {"A": 1.454, "B": 1.5, "C": 3.87},
])

EXPECTED = DataFrame(data=[
    {"A": 1.46, "B": 1.0, "C": 3.873},
    {"A": 1.46, "B": 2.0, "C": 3.874},
    {"A": 1.4, "C": 3.87},
    {"A": 1.45, "B": 2.0, "C": 3.87},
])


def test_rounding():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = RoundRule({"A": 2, "B": 0, "C": 3}, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), EXPECTED)


def test_missing_column_strict():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = RoundRule({"A": 2, "B": 0, "C": 3, "Z": 2}, named_input="input", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Column(s) {'Z'} are missing from the input dataframe."


def test_missing_column_non_strict():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = RoundRule({"A": 2, "B": 0, "C": 3, "Z": 2}, named_input="input", named_output="result", strict=False)
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), EXPECTED)
