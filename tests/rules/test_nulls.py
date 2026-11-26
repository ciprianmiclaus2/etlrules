import pytest

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from tests.utils.data import assert_frame_equal, get_test_data


INPUT_DF = [
    {"A": 1, "B": "b1", "C": 1.5, "D": 10},
    {"B": "b2", "D": 20},
    {"A": 3, "C": 3.5},
    {},
]
INPUT_TYPES = {"A": "Int64", "B": "string", "C": "Float64", "D": "Int64"}


@pytest.mark.parametrize("columns,value,expected", [
    [["A"], 0, [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
        {"A": 0, "B": "b2", "D": 20},
        {"A": 3, "C": 3.5},
        {"A": 0},
    ]],
    [["A", "D"], -1, [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
        {"A": -1, "B": "b2", "D": 20},
        {"A": 3, "C": 3.5, "D": -1},
        {"A": -1, "D": -1},
    ]],
    [["B"], "unknown", [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
        {"B": "b2", "D": 20},
        {"A": 3, "B": "unknown", "C": 3.5},
        {"B": "unknown"},
    ]],
    [["C"], 0.5, [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
        {"B": "b2", "C": 0.5, "D": 20},
        {"A": 3, "C": 3.5},
        {"C": 0.5},
    ]],
])
def test_fill_null(columns, value, expected, backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_TYPES)
    expected = backend.DataFrame(expected, astype=INPUT_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.FillNullRule(columns, value, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_fill_null_empty_df(backend):
    input_df = backend.DataFrame({"A": [], "B": []}, astype={"A": "Int64", "B": "string"})
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.FillNullRule(["A"], 0, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), input_df)


def test_fill_null_missing_column(backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.FillNullRule(["A", "Z"], 0, named_input="input", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert "Z" in str(exc.value)


@pytest.mark.parametrize("columns,how,expected", [
    [None, "any", [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
    ]],
    [["A"], "any", [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
        {"A": 3, "C": 3.5},
    ]],
    [["A", "B"], "any", [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
    ]],
    [["A", "B"], "all", [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
        {"B": "b2", "D": 20},
        {"A": 3, "C": 3.5},
    ]],
    [None, "all", [
        {"A": 1, "B": "b1", "C": 1.5, "D": 10},
        {"B": "b2", "D": 20},
        {"A": 3, "C": 3.5},
    ]],
])
def test_drop_null_rows(columns, how, expected, backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_TYPES)
    expected = backend.DataFrame(expected, astype=INPUT_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DropNullRowsRule(columns, how=how, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_drop_null_rows_missing_column(backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.DropNullRowsRule(["A", "Z"], named_input="input", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert "Z" in str(exc.value)


COALESCE_DF = [
    {"Mobile": 111, "Home": 222},
    {"Home": 444, "Work": 555},
    {"Work": 666},
    {},
]
COALESCE_TYPES = {"Mobile": "Int64", "Home": "Int64", "Work": "Int64"}


@pytest.mark.parametrize("columns,expected", [
    [["Mobile", "Home", "Work"], [111, 444, 666, None]],
    [["Work", "Home", "Mobile"], [222, 555, 666, None]],
    [["Home"], [222, 444, None, None]],
])
def test_coalesce(columns, expected, backend):
    input_df = backend.DataFrame(COALESCE_DF, astype=COALESCE_TYPES)
    expected_df = backend.hconcat(input_df, backend.DataFrame({"Phone": expected}, astype={"Phone": "Int64"}))
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.CoalesceRule(columns, "Phone", named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected_df)


def test_coalesce_strings(backend):
    input_df = backend.DataFrame([{"A": "a1", "B": "b1"}, {"B": "b2"}, {}], astype={"A": "string", "B": "string"})
    expected = backend.hconcat(input_df, backend.DataFrame({"C": ["a1", "b2", None]}, astype={"C": "string"}))
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.CoalesceRule(["A", "B"], "C", named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_coalesce_empty_df(backend):
    input_df = backend.DataFrame({"A": [], "B": []}, astype={"A": "Int64", "B": "Int64"})
    expected = backend.DataFrame({"A": [], "B": [], "C": []}, astype={"A": "Int64", "B": "Int64", "C": "Int64"})
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.CoalesceRule(["A", "B"], "C", named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_coalesce_existing_output_column(backend):
    input_df = backend.DataFrame(COALESCE_DF, astype=COALESCE_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.CoalesceRule(["Mobile", "Home"], "Work", named_input="input", named_output="result")
        with pytest.raises(ColumnAlreadyExistsError):
            rule.apply(data)
    # non-strict: the output column is overwritten
    expected = backend.DataFrame(
        [{"Mobile": 111, "Home": 222, "Work": 111}, {"Home": 444, "Work": 444}, {}, {}], astype=COALESCE_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.CoalesceRule(["Mobile", "Home"], "Work", named_input="input", named_output="result", strict=False)
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


def test_coalesce_missing_column(backend):
    input_df = backend.DataFrame(COALESCE_DF, astype=COALESCE_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.CoalesceRule(["Mobile", "Z"], "Phone", named_input="input", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert "Z" in str(exc.value)
