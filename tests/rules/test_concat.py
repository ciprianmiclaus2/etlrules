import pytest

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError, SchemaError
from tests.utils.data import assert_frame_equal, get_test_data


def test_vconcat_all_cols(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"A": 4, "B": "e", "C": True},
        {"A": 5, "B": "f", "C": False},
        {"A": 6, "B": "g", "C": False},
    ])
    expected = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
        {"A": 4, "B": "e", "C": True},
        {"A": 5, "B": "f", "C": False},
        {"A": 6, "B": "g", "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.VConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_vconcat_empty_df(backend):
    left_df = backend.impl.DataFrame(data={"A": [], "B": [], "C": []})
    right_df = backend.impl.DataFrame(data={"A": [], "B": [], "G": []})
    expected = backend.impl.DataFrame(data={"A": [], "B": [], "C": [], "G": []})
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.VConcatRule(named_input_left="left", named_input_right="right", named_output="result", strict=False)
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_vconcat_subset_cols(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True, "D": "None"},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"A": 4, "B": "e", "C": True, "F": 3.0},
        {"A": 5, "B": "f", "C": False},
        {"A": 6, "B": "g", "C": False},
    ])
    expected = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b"},
        {"A": 2, "B": "c"},
        {"A": 3, "B": "d"},
        {"A": 4, "B": "e"},
        {"A": 5, "B": "f"},
        {"A": 6, "B": "g"},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.VConcatRule(named_input_left="left", named_input_right="right", subset_columns=["A", "B"], named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_vconcat_missing_col_right(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True, "D": "None"},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"A": 4, "C": True, "F": 3.0},
        {"A": 5, "C": False},
        {"A": 6, "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.VConcatRule(named_input_left="left", named_input_right="right", subset_columns=["A", "B"], named_output="result", strict=False)
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Missing columns in the right dataframe of the concat operation: {'B'}"


def test_vconcat_missing_col_left(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True, "D": "None"},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"A": 4, "C": True, "F": 3.0},
        {"A": 5, "C": False},
        {"A": 6, "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.VConcatRule(named_input_left="left", named_input_right="right", subset_columns=["A", "F"], named_output="result", strict=False)
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Missing columns in the left dataframe of the concat operation: {'F'}"


def test_vconcat_different_schema_non_strict(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"A": 4, "C": True, "F": 3.0},
        {"A": 5, "C": False},
        {"A": 6, "C": False},
    ])
    expected = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
        {"A": 4, "C": True, "F": 3.0},
        {"A": 5, "C": False},
        {"A": 6, "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.VConcatRule(named_input_left="left", named_input_right="right", named_output="result", strict=False)
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_vconcat_different_schema_strict(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"A": 4, "C": True, "F": 3.0},
        {"A": 5, "C": False},
        {"A": 6, "C": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.VConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        with pytest.raises(SchemaError) as exc:
            rule.apply(data)
        assert str(exc.value) == "VConcat needs both dataframe have the same schema. Missing columns in the right df: {'F'}. Missing columns in the left df: {'B'}"


def test_hconcat(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"D": 4, "F": "e", "G": True},
        {"D": 5, "F": "f", "G": False},
        {"D": 6, "F": "g", "G": False},
    ])
    expected = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True, "D": 4, "F": "e", "G": True},
        {"A": 2, "B": "c", "C": False, "D": 5, "F": "f", "G": False},
        {"A": 3, "B": "d", "C": True, "D": 6, "F": "g", "G": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.HConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_hconcat_empty_df(backend):
    left_df = backend.impl.DataFrame(data={"A": [], "B": [], "C": []})
    right_df = backend.impl.DataFrame(data={"D": [], "F": [], "G": []})
    expected = backend.impl.DataFrame(data={"A": [], "B": [], "C": [], "D": [], "F": [], "G": []})
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.HConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_hconcat_different_no_rows_strict(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"D": 4, "F": "e", "G": True},
        {"D": 5, "F": "f", "G": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.HConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        with pytest.raises(SchemaError) as exc:
            rule.apply(data)
        assert str(exc.value) == "HConcat needs the two dataframe to have the same number of rows. left df=3 rows, right df=2 rows."


def test_hconcat_different_columns_with_same_name(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"D": 4, "B": "e", "G": True},
        {"D": 5, "B": "f", "G": False},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.HConcatRule(named_input_left="left", named_input_right="right", named_output="result")
        with pytest.raises(ColumnAlreadyExistsError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Column(s) {'B'} exist in both dataframes."


def test_hconcat_different_no_rows_non_strict(backend):
    left_df = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True},
        {"A": 2, "B": "c", "C": False},
        {"A": 3, "B": "d", "C": True},
    ])
    right_df = backend.impl.DataFrame(data=[
        {"D": 4, "F": "e", "G": True},
        {"D": 5, "F": "f", "G": False},
    ])
    expected = backend.impl.DataFrame(data=[
        {"A": 1, "B": "b", "C": True, "D": 4, "F": "e", "G": True},
        {"A": 2, "B": "c", "C": False, "D": 5, "F": "f", "G": False},
        {"A": 3, "B": "d", "C": True},
    ])
    with get_test_data(left_df, named_inputs={"left": left_df, "right": right_df}, named_output="result") as data:
        rule = backend.rules.HConcatRule(named_input_left="left", named_input_right="right", named_output="result", strict=False)
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)
