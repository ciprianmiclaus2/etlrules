import pytest

from etlrules.exceptions import MissingColumnError, ValidationError
from tests.utils.data import assert_frame_equal, get_test_data


INPUT_DF = [
    {"Id": 1, "Name": "a", "Country": "UK"},
    {"Id": 2, "Name": "b", "Country": "UK"},
    {"Id": 3, "Country": "FR"},
    {"Id": 3, "Name": "d"},
]
INPUT_TYPES = {"Id": "Int64", "Name": "string", "Country": "string"}


@pytest.mark.parametrize("columns,error", [
    [["Id"], None],
    [["Name"], "Name: 1"],
    [["Id", "Name", "Country"], "Name: 1, Country: 1"],
    [None, "Name: 1, Country: 1"],
])
def test_assert_not_null(columns, error, backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.AssertNotNullRule(columns, named_input="input", named_output="result")
        if error is None:
            rule.apply(data)
            # the data is passed through unchanged
            assert_frame_equal(data.get_named_output("result"), input_df)
        else:
            with pytest.raises(ValidationError) as exc:
                rule.apply(data)
            assert error in str(exc.value)


@pytest.mark.parametrize("columns,error", [
    [["Name"], None],
    [["Id"], "Found 1 duplicate row(s)"],
    [["Id", "Name"], None],
    [["Country"], "Found 1 duplicate row(s)"],
    [None, None],
])
def test_assert_unique(columns, error, backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.AssertUniqueRule(columns, named_input="input", named_output="result")
        if error is None:
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), input_df)
        else:
            with pytest.raises(ValidationError) as exc:
                rule.apply(data)
            assert error in str(exc.value)


def test_assert_unique_missing_values_are_duplicates(backend):
    # two rows with a missing value in the column are duplicates
    input_df = backend.DataFrame([{"A": 1}, {}, {}], astype={"A": "Int64"})
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        with pytest.raises(ValidationError) as exc:
            backend.rules.AssertUniqueRule(["A"], named_input="input", named_output="result").apply(data)
        assert "Found 1 duplicate row(s)" in str(exc.value)


@pytest.mark.parametrize("rule_cls_str", ["AssertNotNullRule", "AssertUniqueRule"])
def test_validation_empty_df(rule_cls_str, backend):
    input_df = backend.DataFrame({"A": []}, astype={"A": "Int64"})
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        getattr(backend.rules, rule_cls_str)(["A"], named_input="input", named_output="result").apply(data)
        assert_frame_equal(data.get_named_output("result"), input_df)


@pytest.mark.parametrize("rule_cls_str", ["AssertNotNullRule", "AssertUniqueRule"])
def test_validation_missing_column(rule_cls_str, backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        with pytest.raises(MissingColumnError):
            getattr(backend.rules, rule_cls_str)(["Z"], named_input="input", named_output="result").apply(data)
