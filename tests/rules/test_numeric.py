from pandas import DataFrame
import pytest

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas import AbsRule, RoundRule
from tests.utils.data import assert_frame_equal, get_test_data

INPUT_DF = [
    {"A": 1.456, "B": 1.456, "C": 3.8734},
    {"A": 1.455, "B": 1.677, "C": 3.8739},
    {"A": 1.4, "C": 3.87},
    {"A": 1.454, "B": 1.5, "C": 3.87},
]

EXPECTED = [
    {"A": 1.46, "B": 1.0, "C": 3.873},
    {"A": 1.46, "B": 2.0, "C": 3.874},
    {"A": 1.4, "C": 3.87},
    {"A": 1.45, "B": 2.0, "C": 3.87},
]

EXPECTED_2 = [
    {"A": 1.46, "B": 1.46, "C": 3.87},
    {"A": 1.46, "B": 1.68, "C": 3.87},
    {"A": 1.4, "C": 3.87},
    {"A": 1.45, "B": 1.5, "C": 3.87},
]

EXPECTED_3 = [
    {"A": 1.456, "B": 1.456, "C": 3.8734, "E": 1.46, "F": 1.46, "G": 3.87},
    {"A": 1.455, "B": 1.677, "C": 3.8739, "E": 1.46, "F": 1.68, "G": 3.87},
    {"A": 1.4, "C": 3.87, "E": 1.4, "G": 3.87},
    {"A": 1.454, "B": 1.5, "C": 3.87, "E": 1.45, "F": 1.5, "G": 3.87},
]

INPUT_DF2 = [
    {"A": "a", "B": 1.456, "C": "c", "D": -100},
    {"A": "b", "B": -1.677, "C": "d"},
    {"A": "c", "C": 3.87, "D": -499},
    {"A": "d", "B": -1.5, "C": "e", "D": 1},
]
EXPECTED2 = [
    {"A": "a", "B": 1.456, "C": "c", "D": 100},
    {"A": "b", "B": 1.677, "C": "d"},
    {"A": "c", "C": 3.87, "D": 499},
    {"A": "d", "B": 1.5, "C": "e", "D": 1},
]

INPUT_DF3 = [
    {"A": "a", "B": 1.456, "C": "c", "D": -100},
    {"A": "b", "B": -1.677, "C": "d"},
    {"A": "c", "C": "x", "D": -499},
    {"A": "d", "B": -1.5, "C": "e", "D": 1},
]
EXPECTED3 = [
    {"A": "a", "B": 1.456, "C": "c", "D": -100, "E": 1.456, "F": 100},
    {"A": "b", "B": -1.677, "C": "d", "E": 1.677},
    {"A": "c", "C": "x", "D": -499, "F": 499},
    {"A": "d", "B": -1.5, "C": "e", "D": 1, "E": 1.5, "F": 1},
]


def test_rounding(backend):
    input_df = backend.impl.DataFrame(data=INPUT_DF)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.RoundRule("A", 2, named_input="input", named_output="result2")
        rule.apply(data)
        rule = backend.rules.RoundRule("B", 0, named_input="result2", named_output="result1")
        rule.apply(data)
        rule = backend.rules.RoundRule("C", 3, named_input="result1", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), backend.impl.DataFrame(data=EXPECTED))


def test_rounding2(backend):
    input_df = backend.impl.DataFrame(data=INPUT_DF)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.RoundRule("A", 2, named_input="input", named_output="result2")
        rule.apply(data)
        rule = backend.rules.RoundRule("B", 2, named_input="result2", named_output="result1")
        rule.apply(data)
        rule = backend.rules.RoundRule("C", 2, named_input="result1", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), backend.impl.DataFrame(data=EXPECTED_2))


def test_rounding3(backend):
    input_df = backend.impl.DataFrame(data=INPUT_DF)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.RoundRule("A", 2, output_column="E", named_input="input", named_output="result2")
        rule.apply(data)
        rule = backend.rules.RoundRule("B", 2, output_column="F", named_input="result2", named_output="result1")
        rule.apply(data)
        rule = backend.rules.RoundRule("C", 2, output_column="G", named_input="result1", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), backend.impl.DataFrame(data=EXPECTED_3))


def test_rounding_missing_column(backend):
    input_df = backend.impl.DataFrame(data=INPUT_DF)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.RoundRule("Z", 2, named_input="input", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Column 'Z' is missing from the input dataframe."


def test_rounding_empty_df(backend):
    input_df = backend.DataFrame(data={"A": []}, dtype="float64")
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.RoundRule("A", 2, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), input_df)


def test_abs(backend):
    input_df2 = backend.impl.DataFrame(data=INPUT_DF2)
    with get_test_data(input_df2, named_inputs={"input": input_df2}, named_output="result") as data:
        rule = backend.rules.AbsRule("B", named_input="input", named_output="result2")
        rule.apply(data)
        rule = backend.rules.AbsRule("D", named_input="result2", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), backend.impl.DataFrame(data=EXPECTED2))


def test_abs_output_columns(backend):
    input_df = backend.impl.DataFrame(data=INPUT_DF3)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.AbsRule("B", output_column="E", named_input="input", named_output="result2")
        rule.apply(data)
        rule = backend.rules.AbsRule("D", output_column="F", named_input="result2", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), backend.impl.DataFrame(data=EXPECTED3))


def test_abs_missing_column(backend):
    input_df = backend.impl.DataFrame(data=INPUT_DF2)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.AbsRule("Z", named_input="input", named_output="result", strict=False)
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Column 'Z' is missing from the input dataframe."


def test_abs_empty_df(backend):
    input_df = backend.DataFrame(data={"A": []}, dtype="Int64")
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.AbsRule("A", named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), input_df)
