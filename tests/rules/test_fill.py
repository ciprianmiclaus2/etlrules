import pytest

from etlrules.exceptions import MissingColumnError
from tests.utils.data import assert_frame_equal, get_test_data


EMPTY_DF = {"A": [], "B": [], "C": [], "D": []}

SAMPLE_DF = [
    {"A": 10, "B": "a"},
    {"A": 15, "D": 9},
    {"A": 12, "B": "d", "C": "e"},
    {"A": 5, "C": "c", "D": 5},
    {"A": 1, "B": "e"},
    {"A": 20, "B": "f"},
]


SAMPLE_GROUPING_DF = [
    {"A": 10, "G": 1, "H": "H1", "B": "a"},
    {"A": 15, "G": 2, "H": "H1", "D": 9},
    {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
    {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
    {"A": 1, "G": 1, "H": "H2", "B": "e"},
    {"A": 20, "G": 2, "H": "H1", "B": "f"},
]


@pytest.mark.parametrize("rule_cls_str,input_sample,columns,sort_by,sort_ascending,group_by,expected", [
    ["ForwardFillRule", EMPTY_DF, ["C", "D"], None, True, None, EMPTY_DF],
    ["ForwardFillRule", SAMPLE_DF, ["C", "D"], None, True, None, [
        {"A": 10, "B": "a"},
        {"A": 15, "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 9},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e", "C": "c", "D": 5},
        {"A": 20, "B": "f", "C": "c", "D": 5},
    ]],
    ["ForwardFillRule", SAMPLE_DF, ["C", "D"], ["A"], True, None, [
        {"A": 1, "B": "e"},
        {"A": 5, "C": "c", "D": 5},
        {"A": 10, "B": "a", "C": "c", "D": 5},
        {"A": 12, "B": "d", "C": "e", "D": 5},
        {"A": 15, "C": "e", "D": 9},
        {"A": 20, "B": "f", "C": "e", "D": 9},
    ]],
    ["ForwardFillRule", SAMPLE_DF, ["C", "D"], ["A"], False, None, [
        {"A": 20, "B": "f"},
        {"A": 15, "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 9},
        {"A": 10, "B": "a", "C": "e", "D": 9},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e", "C": "c", "D": 5},
    ]],
    ["ForwardFillRule", SAMPLE_GROUPING_DF, ["C", "D"], None, True, ["G", "H"], [
        {"A": 10, "G": 1, "H": "H1", "B": "a"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 20, "G": 2, "H": "H1", "B": "f", "D": 9},
    ]],
    ["ForwardFillRule", SAMPLE_GROUPING_DF, ["C", "D"], ["A"], True, ["G", "H"], [
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 10, "G": 1, "H": "H1", "B": "a"},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 20, "G": 2, "H": "H1", "B": "f", "D": 9},
    ]],
    ["ForwardFillRule", SAMPLE_GROUPING_DF, ["C", "D"], ["A"], False, ["G", "H"], [
        {"A": 20, "G": 2, "H": "H1", "B": "f"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 10, "G": 1, "H": "H1", "B": "a", "C": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
    ]],
    ["BackFillRule", EMPTY_DF, ["C", "D"], None, True, None, EMPTY_DF],
    ["BackFillRule", SAMPLE_DF, ["C", "D"], None, True, None, [
        {"A": 10, "B": "a",  "C": "e", "D": 9},
        {"A": 15,  "C": "e", "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 5},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e"},
        {"A": 20, "B": "f"},
    ]],
    ["BackFillRule", SAMPLE_DF, ["C", "D"], ["A"], True, None, [
        {"A": 1, "B": "e", "C": "c", "D": 5},
        {"A": 5, "C": "c", "D": 5},
        {"A": 10, "B": "a", "C": "e", "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 9},
        {"A": 15, "D": 9},
        {"A": 20, "B": "f"},
    ]],
    ["BackFillRule", SAMPLE_DF, ["C", "D"], ["A"], False, None, [
        {"A": 20, "B": "f", "C": "e", "D": 9},
        {"A": 15, "C": "e", "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 5},
        {"A": 10, "B": "a", "C": "c", "D": 5},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e"},
    ]],
    ["BackFillRule", SAMPLE_GROUPING_DF, ["C", "D"], None, True, ["G", "H"], [
        {"A": 10, "G": 1, "H": "H1", "B": "a", "C": "e"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 20, "G": 2, "H": "H1", "B": "f"},
    ]],
    ["BackFillRule", SAMPLE_GROUPING_DF, ["C", "D"], ["A"], True, ["G", "H"], [
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 10, "G": 1, "H": "H1", "B": "a", "C": "e"},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 20, "G": 2, "H": "H1", "B": "f"},
    ]],
    ["BackFillRule", SAMPLE_GROUPING_DF, ["C", "D"], ["A"], False, ["G", "H"], [
        {"A": 20, "G": 2, "H": "H1", "B": "f", "D": 9},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 10, "G": 1, "H": "H1", "B": "a"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
    ]],
])
def test_forward_fill_rule_simple(rule_cls_str, input_sample, columns, sort_by, sort_ascending, group_by, expected, backend):
    sample_df = backend.DataFrame(input_sample)
    expected = backend.DataFrame(expected) if isinstance(expected, (list, dict)) else expected
    before_column_order = [col for col in sample_df.columns]
    with get_test_data(sample_df, named_inputs={"payload": sample_df}, named_output="result") as data:
        rule_cls = getattr(backend.rules, rule_cls_str)
        rule = rule_cls(columns, sort_by, sort_ascending, group_by, named_input="payload", named_output="result")
        rule.apply(data)
        expected = expected[before_column_order]
        result = data.get_named_output("result")
        after_column_order = [col for col in result.columns]
        assert before_column_order == after_column_order
        assert_frame_equal(result, expected, ignore_row_ordering=True)


@pytest.mark.parametrize("rule_cls_str", ["ForwardFillRule", "BackFillRule"])
def test_missing_sort_by_column(rule_cls_str, backend):
    rule_cls = getattr(backend.rules, rule_cls_str)
    sample_df = backend.DataFrame(SAMPLE_DF)
    with get_test_data(sample_df, named_inputs={"payload": sample_df}, named_output="result") as data:
        rule = rule_cls(["C", "D"], sort_by=["E", "A"], named_input="payload", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Missing sort_by column(s) in fill operation: {'E'}"


@pytest.mark.parametrize("rule_cls_str", ["ForwardFillRule", "BackFillRule"])
def test_missing_group_by_column(rule_cls_str, backend):
    rule_cls = getattr(backend.rules, rule_cls_str)
    sample_df = backend.DataFrame(SAMPLE_DF)
    with get_test_data(sample_df, named_inputs={"payload": sample_df}, named_output="result") as data:
        rule = rule_cls(["C", "D"], group_by=["E", "A"], named_input="payload", named_output="result")
        with pytest.raises(MissingColumnError) as exc:
            rule.apply(data)
        assert str(exc.value) == "Missing group_by column(s) in fill operation: {'E'}"
