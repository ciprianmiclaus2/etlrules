import pytest

from etlrules.exceptions import (
    ColumnAlreadyExistsError,
    MissingColumnError,
    ExpressionSyntaxError,
    UnsupportedTypeError,
)

from tests.utils.data import assert_frame_equal, get_test_data


INPUT_DF = [
    {"A": 1, "B": "b", "C": 1, "D": "a", "E": 1, "F": "a"},
    {"A": 2, "B": "b", "C": 2, "D": "b", "F": "b"},
    {"A": 1, "B": "b", "C": 3, "D": "c"},
    {"A": 2, "B": "b", "C": 4, "D": "d", "E": 2},
    {"A": 3, "B": "b", "C": 5, "D": "e"},
    {"A": 2, "B": "b", "C": 6, "D": "f"},
]
INPUT_DF_TYPES = {
    "A": "Int64", "B": "string", "C": "Int64", "D": "string", "E": "Int64", "F": "string"
}

INPUT_EMPTY_DF = {"A": [], "B": [], "C": [], "D": [], "E": [], "F": []}

SCENARIOS = [
    [["A", "B"], {"C": "sum", "D": "max", "E": "min", "F": "first"}, None, None, [
        {"A": 1, "B": "b", "C": 4, "D": "c", "E": 1, "F": "a"},
        {"A": 2, "B": "b", "C": 12, "D": "f", "E": 2, "F": "b"},
        {"A": 3, "B": "b", "C": 5, "D": "e"},
    ], INPUT_DF_TYPES],
    [
        ["B"],
        {"C": "sum", "D": "max", "E": "min", "F": "first"},
        None, None,
        [
            {"B": "b", "C": 21, "D": "f", "E": 1, "F": "a"},
        ],
        {
            "B": "string", "C": "Int64", "D": "string", "E": "Int64", "F": "string"
        }
    ],
    [["A", "B"], {"C": "count", "D": "last", "E": "count", "F": "countNoNA"}, None, {"C": "int64", "E": "int64", "F": "int64"}, [
        {"A": 1, "B": "b", "C": 2, "D": "c", "E": 2, "F": 1},
        {"A": 2, "B": "b", "C": 3, "D": "f", "E": 3, "F": 1},
        {"A": 3, "B": "b", "C": 1, "D": "e", "E": 1, "F": 0},
    ], {
        "A": "Int64", "B": "string", "C": "Int64", "D": "string", "E": "Int64", "F": "Int64"
    }],
    [["B"], {"C": "count", "D": "last", "E": "count", "F": "countNoNA"}, None, {"C": "int64", "E": "int64", "F": "int64"}, [
        {"B": "b", "C": 6, "D": "f", "E": 6, "F": 2},
    ], {
        "B": "string", "C": "Int64", "D": "string", "E": "Int64", "F": "Int64"
    }],
    [["A", "B"], {"C": "list", "D": "list", "E": "list", "F": "list"}, None, None, [
        {"A": 1, "B": "b", "C": [1, 3], "D": ["a", "c"], "E": [1], "F": ["a"]},
        {"A": 2, "B": "b", "C": [2, 4, 6], "D": ["b", "d", "f"], "E": [2], "F": ["b"]},
        {"A": 3, "B": "b", "C": [5], "D": ["e"], "E": [], "F": []},
    ], {
        "A": "Int64", "B": "string", "C": "list_int64s", "D": "list_strings", "E": "list_int64s", "F": "list_strings"
    }],
    [["B"], {"C": "list", "D": "list", "E": "list", "F": "list"}, None, None, [
        {"B": "b", "C": [1, 2, 3, 4, 5, 6], "D": ["a", "b", "c", "d", "e", "f"], "E": [1, 2], "F": ["a", "b"]},
    ], {
        "B": "string", "C": "list_int64s", "D": "list_strings", "E": "list_int64s", "F": "list_strings"
    }],
    [["A", "B"], {"C": "csv", "D": "csv", "E": "csv", "F": "csv"}, None, {"C": "string", "D": "string", "E": "string"}, [
        {"A": 1, "B": "b", "C": "1,3", "D": "a,c", "E": "1", "F": "a"},
        {"A": 2, "B": "b", "C": "2,4,6", "D": "b,d,f", "E": "2", "F": "b"},
        {"A": 3, "B": "b", "C": "5", "D": "e", "E": "", "F": ""},
    ], {
        "A": "Int64", "B": "string", "C": "string", "D": "string", "E": "string", "F": "string"
    }],
    [["B"], {"C": "csv", "D": "csv", "E": "csv", "F": "csv"}, None, {"C": "string", "E": "string", "F": "string"}, [
        {"B": "b", "C": "1,2,3,4,5,6", "D": "a,b,c,d,e,f", "E": "1,2", "F": "a,b"},
    ], {
        "B": "string", "C": "string", "D": "string", "E": "string", "F": "string"
    }],
    [["A", "B"], None, {"C": "sum(v**2 for v in values)", "D": "';'.join(values)", "E": "int(sum(v**2 for v in values if not isnull(v)))", "F": "':'.join(v for v in values if not isnull(v))"}, 
    None, [
        {"A": 1, "B": "b", "C": 10, "D": "a;c", "E": 1, "F": "a"},
        {"A": 2, "B": "b", "C": 56, "D": "b;d;f", "E": 4, "F": "b"},
        {"A": 3, "B": "b", "C": 25, "D": "e", "E": 0, "F": ""},
    ], {
        "A": "Int64", "B": "string", "C": "Int64", "D": "string", "E": "Int64", "F": "string"
    }],
]


@pytest.mark.parametrize(
    "group_by,aggregations,aggregation_expressions,aggregation_types,expected,expected_astype", SCENARIOS
)
def test_aggregate_scenarios(
    group_by, aggregations, aggregation_expressions, aggregation_types, expected, expected_astype, backend
):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_DF_TYPES)
    expected = backend.DataFrame(expected, astype=expected_astype)
    with get_test_data(
        input_df, named_inputs={"input": input_df}, named_output="result"
    ) as data:
        rule = backend.rules.AggregateRule(
            group_by,
            aggregations=aggregations,
            aggregation_expressions=aggregation_expressions,
            aggregation_types=aggregation_types,
            named_input="input",
            named_output="result",
        )
        rule.apply(data)
        actual = data.get_named_output("result")
        assert_frame_equal(actual, expected)


def test_aggregate_empty_df(backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_DF_TYPES)
    input_empty_df = backend.DataFrame(INPUT_EMPTY_DF, astype=INPUT_DF_TYPES)
    with get_test_data(
        input_df, named_inputs={"input": input_empty_df}, named_output="result"
    ) as data:
        rule = backend.rules.AggregateRule(
            ["A", "B"],
            aggregations={"C": "sum", "D": "max", "E": "min", "F": "first"},
            aggregation_expressions=None,
            aggregation_types={"D": "string"},
            named_input="input",
            named_output="result",
        )
        rule.apply(data)
        actual = data.get_named_output("result")
        assert_frame_equal(actual, input_empty_df)


@pytest.mark.parametrize(
    "group_by,aggregations,aggregation_expressions,aggregation_types,strict,expected_exc,expected_exc_str", [
        [["A", "B"], {"C": "min", "A": "max"}, None, None, False, ColumnAlreadyExistsError, "Column A appears in group_by and cannot be aggregated."],
        [["A", "B"], None, {"C": "min", "A": "max"}, None, False, ColumnAlreadyExistsError, "Column A appears in group_by and cannot be aggregated."],
        [["A", "B"], {"C": "max", "D": "first"}, {"Z": "last", "C": "min"}, None, False, ColumnAlreadyExistsError, "Column C is already being aggregated."],
        [["A", "B"], {"C": "unknown"}, None, None, False, ValueError, "'unknown' is not a supported aggregation function."],
        [["A", "B"], None, {"C": "a + b + "}, None, False, SyntaxError, "Error in aggregation expression for column 'C': 'a + b + '"],
        [["A", "B"], {"D": "min"}, {"C": "a + b"}, {"D": "int64", "C": "int64", "Z": "string"}, True, MissingColumnError, "Column Z is neither in the group by columns nor in the aggregations."],
        [["A", "B"], {"D": "min"}, {"C": "a + b"}, {"Z": "int64", "Y": "int64", "C": "unknown"}, False, UnsupportedTypeError, "Unsupported type 'unknown' for column 'C'."],
        [["A", "B"], {"Y": "min"}, None, None, True, MissingColumnError, "Missimg columns to aggregate by: {'Y'}"],
    ]
)
def test_aggregate_exc_scenarios(group_by, aggregations, aggregation_expressions, aggregation_types, strict, expected_exc, expected_exc_str, backend):
    input_df = backend.DataFrame(INPUT_DF, astype=INPUT_DF_TYPES)
    input_empty_df = backend.DataFrame(INPUT_EMPTY_DF, astype=INPUT_DF_TYPES)
    with get_test_data(input_df, named_inputs={"input": input_empty_df}, named_output="result") as data:
        with pytest.raises(expected_exc) as exc:
            rule = backend.rules.AggregateRule(
                group_by=group_by,
                aggregations=aggregations,
                aggregation_expressions=aggregation_expressions,
                aggregation_types=aggregation_types,
                named_input="input",
                named_output="result",
                strict=strict,
            )
            rule.apply(data)
        if expected_exc_str:
            assert expected_exc_str in str(exc.value)
