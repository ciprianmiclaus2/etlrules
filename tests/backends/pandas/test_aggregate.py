import numpy as np
from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.backends.pandas import AggregateRule
from tests.backends.pandas.utils.data import get_test_data


INPUT_DF = DataFrame(data=[
    {"A": 1, "B": "b", "C": 1, "D": "a", "E": 1, "F": "a"},
    {"A": 2, "B": "b", "C": 2, "D": "b", "F": "b"},
    {"A": 1, "B": "b", "C": 3, "D": "c"},
    {"A": 2, "B": "b", "C": 4, "D": "d", "E": 2},
    {"A": 3, "B": "b", "C": 5, "D": "e"},
    {"A": 2, "B": "b", "C": 6, "D": "f"},
])

INPUT_EMPTY_DF = DataFrame(data={"A": [], "B": [], "C": [], "D": [], "E": [], "F": []}).astype({"A": "Int64", "B": "string", "C": "Int64", "D": "string", "E": "Int64", "F": "string"})

SCENARIOS = [
    [["A", "B"], {"C": "sum", "D": "max", "E": "min", "F": "first"}, None, DataFrame(data=[
        {"A": 1, "B": "b", "C": 4, "D": "c", "E": 1, "F": "a"},
        {"A": 2, "B": "b", "C": 12, "D": "f", "E": 2, "F": "b"},
        {"A": 3, "B": "b", "C": 5, "D": "e"},
    ])],
    [["B"], {"C": "sum", "D": "max", "E": "min", "F": "first"}, None, DataFrame(data=[
        {"B": "b", "C": 21, "D": "f", "E": 1.0, "F": "a"},
    ])],
    [["A", "B"], {"C": "count", "D": "last", "E": "count", "F": "countNoNA"}, None, DataFrame(data=[
        {"A": 1, "B": "b", "C": 2, "D": "c", "E": 2, "F": 1},
        {"A": 2, "B": "b", "C": 3, "D": "f", "E": 3, "F": 1},
        {"A": 3, "B": "b", "C": 1, "D": "e", "E": 1, "F": 0},
    ])],
    [["B"], {"C": "count", "D": "last", "E": "count", "F": "countNoNA"}, None, DataFrame(data=[
        {"B": "b", "C": 6, "D": "f", "E": 6, "F": 2},
    ])],
    [["A", "B"], {"C": "list", "D": "list", "E": "list", "F": "list"}, None, DataFrame(data=[
        {"A": 1, "B": "b", "C": [1, 3], "D": ["a", "c"], "E": [1], "F": ["a"]},
        {"A": 2, "B": "b", "C": [2, 4, 6], "D": ["b", "d", "f"], "E": [2], "F": ["b"]},
        {"A": 3, "B": "b", "C": [5], "D": ["e"], "E": [], "F": []},
    ])],
    [["B"], {"C": "list", "D": "list", "E": "list", "F": "list"}, None, DataFrame(data=[
        {"B": "b", "C": [1, 2, 3, 4, 5, 6], "D": ["a", "b", "c", "d", "e", "f"], "E": [1.0, 2.0], "F": ["a", "b"]},
    ])],
    [["A", "B"], {"C": "tuple", "D": "tuple", "E": "tuple", "F": "tuple"}, None, DataFrame(data=[
        {"A": 1, "B": "b", "C": (1, 3), "D": ("a", "c"), "E": (1, ), "F": ("a", )},
        {"A": 2, "B": "b", "C": (2, 4, 6), "D": ("b", "d", "f"), "E": (2, ), "F": ("b", )},
        {"A": 3, "B": "b", "C": (5, ), "D": ("e", ), "E": (), "F": ()},
    ])],
    [["B"], {"C": "tuple", "D": "tuple", "E": "tuple", "F": "tuple"}, None, DataFrame(data=[
        {"B": "b", "C": (1, 2, 3, 4, 5, 6), "D": ("a", "b", "c", "d", "e", "f"), "E": (1, 2), "F": ("a", "b")},
    ])],
    [["A", "B"], {"C": "csv", "D": "csv", "E": "csv", "F": "csv"}, None, DataFrame(data=[
        {"A": 1, "B": "b", "C": "1,3", "D": "a,c", "E": "1.0", "F": "a"},
        {"A": 2, "B": "b", "C": "2,4,6", "D": "b,d,f", "E": "2.0", "F": "b"},
        {"A": 3, "B": "b", "C": "5", "D": "e", "E": "", "F": ""},
    ])],
    [["B"], {"C": "csv", "D": "csv", "E": "csv", "F": "csv"}, None, DataFrame(data=[
        {"B": "b", "C": "1,2,3,4,5,6", "D": "a,b,c,d,e,f", "E": "1.0,2.0", "F": "a,b"},
    ])],
    [["A", "B"], None, {"C": "sum(v**2 for v in values)", "D": "';'.join(values)", "E": "int(sum(v**2 for v in values if not isnull(v)))", "F": "':'.join(v for v in values if not isnull(v))"}, DataFrame(data=[
        {"A": 1, "B": "b", "C": 10, "D": "a;c", "E": 1, "F": "a"},
        {"A": 2, "B": "b", "C": 56, "D": "b;d;f", "E": 4, "F": "b"},
        {"A": 3, "B": "b", "C": 25, "D": "e", "E": 0, "F": ""},
    ])],
]


@pytest.mark.parametrize("group_by,aggregations,aggregation_expressions,expected", SCENARIOS)
def test_aggregate_scenarios(group_by, aggregations, aggregation_expressions, expected):
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_DF}, named_output="result") as data:
        rule = AggregateRule(group_by, aggregations=aggregations, aggregation_expressions=aggregation_expressions, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), expected)


@pytest.mark.skip("needs fixing")
def test_aggregate_empty_df():
    with get_test_data(INPUT_DF, named_inputs={"input": INPUT_EMPTY_DF}, named_output="result") as data:
        rule = AggregateRule(
            ["A", "B"],
            aggregations={"C": "sum", "D": "max", "E": "min", "F": "first"},
            aggregation_expressions=None, named_input="input", named_output="result")
        rule.apply(data)
        assert_frame_equal(data.get_named_output("result"), INPUT_EMPTY_DF)
