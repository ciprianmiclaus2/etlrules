from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from finrules.backends.pandas import VConcatRule
from finrules.exceptions import MissingColumnError, SchemaError
from tests.backends.pandas.utils.data import get_test_data


def test_vconcat_all_cols():
    left_df = DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = DataFrame(data=[
        {"A": 4, "B": "e", "C": True},
        {"A": 5, "B": "f", "C": False},
        {"A": 6, "B": "g", "C": False},
    ])
    expected = DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
        {"A": 4, "B": "e", "C": True},
        {"A": 5, "B": "f", "C": False},
        {"A": 6, "B": "g", "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = VConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_vconcat_subset_cols():
    left_df = DataFrame(data=[
        {"A": 1, "B": "b", "C": True, "D": "None"},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = DataFrame(data=[
        {"A": 4, "B": "e", "C": True, "F": 3.0},
        {"A": 5, "B": "f", "C": False},
        {"A": 6, "B": "g", "C": False},
    ])
    expected = DataFrame(data=[
        {"A": 1, "B": "b"},
        {"A": 2, "B": "c"},
        {"A": 3, "B": "d"},
        {"A": 4, "B": "e"},
        {"A": 5, "B": "f"},
        {"A": 6, "B": "g"},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = VConcatRule(named_input_left="left", named_input_right="right", subset_columns=["A", "B"], named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_vconcat_missing_col():
    left_df = DataFrame(data=[
        {"A": 1, "B": "b", "C": True, "D": "None"},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = DataFrame(data=[
        {"A": 4, "C": True, "F": 3.0},
        {"A": 5, "C": False},
        {"A": 6, "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = VConcatRule(named_input_left="left", named_input_right="right", subset_columns=["A", "B"], named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Missing columns in the right dataframe of the concat operation: {'B'}"


def test_vconcat_different_schema_strict():
    left_df = DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = DataFrame(data=[
        {"A": 4, "C": True, "F": 3.0},
        {"A": 5, "C": False},
        {"A": 6, "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = VConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        with pytest.raises(SchemaError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Concat needs both dataframe have the same schema. Missing columns in the right df: {'F'}. Missing columns in the left df: {'B'}"
