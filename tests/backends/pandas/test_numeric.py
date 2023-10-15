from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas import AbsRule, RoundRule
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

EXPECTED_2 = DataFrame(data=[
    {"A": 1.46, "B": 1.46, "C": 3.87},
    {"A": 1.46, "B": 1.68, "C": 3.87},
    {"A": 1.4, "C": 3.87},
    {"A": 1.45, "B": 1.5, "C": 3.87},
])

EXPECTED_3 = DataFrame(data=[
    {"A": 1.456, "B": 1.456, "C": 3.8734, "E": 1.46, "F": 1.46, "G": 3.87},
    {"A": 1.455, "B": 1.677, "C": 3.8739, "E": 1.46, "F": 1.68, "G": 3.87},
    {"A": 1.4, "C": 3.87, "E": 1.4, "G": 3.87},
    {"A": 1.454, "B": 1.5, "C": 3.87, "E": 1.45, "F": 1.5, "G": 3.87},
])

INPUT_DF2 = DataFrame(data=[
    {"A": "a", "B": 1.456, "C": "c", "D": -100},
    {"A": "b", "B": -1.677, "C": "d"},
    {"A": "c", "C": 3.87, "D": -499},
    {"A": "d", "B": -1.5, "C": "e", "D": 1},
])
EXPECTED2 = DataFrame(data=[
    {"A": "a", "B": 1.456, "C": "c", "D": 100},
    {"A": "b", "B": 1.677, "C": "d"},
    {"A": "c", "C": 3.87, "D": 499},
    {"A": "d", "B": 1.5, "C": "e", "D": 1},
])

INPUT_DF3 = DataFrame(data=[
    {"A": "a", "B": 1.456, "C": "c", "D": -100},
    {"A": "b", "B": -1.677, "C": "d"},
    {"A": "c", "C": "x", "D": -499},
    {"A": "d", "B": -1.5, "C": "e", "D": 1},
])
EXPECTED3 = DataFrame(data=[
    {"A": "a", "B": 1.456, "C": "c", "D": -100, "E": 1.456, "F": 100},
    {"A": "b", "B": -1.677, "C": "d", "E": 1.677},
    {"A": "c", "C": "x", "D": -499, "F": 499},
    {"A": "d", "B": -1.5, "C": "e", "D": 1, "E": 1.5, "F": 1},
])


def test_rounding():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = RoundRule("A", 2, named_input="input", named_output="result2")
        rule.apply(data)
        rule = RoundRule("B", 0, named_input="result2", named_output="result1")
        rule.apply(data)
        rule = RoundRule("C", 3, named_input="result1", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), EXPECTED)


def test_rounding2():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = RoundRule("A", 2, named_input="input", named_output="result2")
        rule.apply(data)
        rule = RoundRule("B", 2, named_input="result2", named_output="result1")
        rule.apply(data)
        rule = RoundRule("C", 2, named_input="result1", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), EXPECTED_2)


def test_rounding3():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = RoundRule("A", 2, output_column="E", named_input="input", named_output="result2")
        rule.apply(data)
        rule = RoundRule("B", 2, output_column="F", named_input="result2", named_output="result1")
        rule.apply(data)
        rule = RoundRule("C", 2, output_column="G", named_input="result1", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), EXPECTED_3)


def test_rounding_missing_column():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = RoundRule("Z", 2, named_input="input", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Column 'Z' is missing from the input dataframe."


def test_rounding_empty_df():
    input_df = DataFrame(data={"A": []}, dtype="float64")
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = RoundRule("A", 2, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), input_df)


def test_abs():
    with get_test_data(INPUT_DF2, named_inputs={"input": INPUT_DF2}, named_output="result") as data:
        rule = AbsRule("B", named_input="input", named_output="result2")
        rule.apply(data)
        rule = AbsRule("D", named_input="result2", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), EXPECTED2)


def test_abs_output_columns():
    with get_test_data(INPUT_DF3, named_inputs={"input": INPUT_DF3}, named_output="result") as data:
        rule = AbsRule("B", output_column="E", named_input="input", named_output="result2")
        rule.apply(data)
        rule = AbsRule("D", output_column="F", named_input="result2", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), EXPECTED3)


def test_abs_missing_column():
    with get_test_data(INPUT_DF2, named_inputs={"input": INPUT_DF2}, named_output="result") as data:
        rule = AbsRule("Z", named_input="input", named_output="result", strict=False)
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Column 'Z' is missing from the input dataframe."


def test_abs_empty_df():
    input_df = DataFrame(data={"A": []}, dtype="Int64")
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = AbsRule("A", named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), input_df)
