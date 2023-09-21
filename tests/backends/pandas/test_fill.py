from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest
from finrules.data import RuleData
from finrules.exceptions import MissingColumn
from finrules.backends.pandas import ForwardFillRule, BackFillRule


SAMPLE_DF = DataFrame(data=[
    {"A": 10, "B": "a"},
    {"A": 15, "D": 9},
    {"A": 12, "B": "d", "C": "e"},
    {"A": 5, "C": "c", "D": 5},
    {"A": 1, "B": "e"},
    {"A": 20, "B": "f"},
])


SAMPLE_GROUPING_DF = DataFrame(data=[
    {"A": 10, "G": 1, "H": "H1", "B": "a"},
    {"A": 15, "G": 2, "H": "H1", "D": 9},
    {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
    {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
    {"A": 1, "G": 1, "H": "H2", "B": "e"},
    {"A": 20, "G": 2, "H": "H1", "B": "f"},
])


@pytest.mark.parametrize("rule_cls,input_sample,columns,sort_by,sort_ascending,group_by,expected", [
    [ForwardFillRule, SAMPLE_DF, ["C", "D"], None, True, None, DataFrame(data=[
        {"A": 10, "B": "a"},
        {"A": 15, "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 9},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e", "C": "c", "D": 5},
        {"A": 20, "B": "f", "C": "c", "D": 5},
    ])],
    [ForwardFillRule, SAMPLE_DF, ["C", "D"], ["A"], True, None, DataFrame(data=[
        {"A": 1, "B": "e"},
        {"A": 5, "C": "c", "D": 5},
        {"A": 10, "B": "a", "C": "c", "D": 5},
        {"A": 12, "B": "d", "C": "e", "D": 5},
        {"A": 15, "C": "e", "D": 9},
        {"A": 20, "B": "f", "C": "e", "D": 9},
    ])],
    [ForwardFillRule, SAMPLE_DF, ["C", "D"], ["A"], False, None, DataFrame(data=[
        {"A": 20, "B": "f"},
        {"A": 15, "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 9},
        {"A": 10, "B": "a", "C": "e", "D": 9},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e", "C": "c", "D": 5},
    ])],
    [ForwardFillRule, SAMPLE_GROUPING_DF, ["C", "D"], None, True, ["G", "H"], DataFrame(data=[
        {"A": 10, "G": 1, "H": "H1", "B": "a"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 20, "G": 2, "H": "H1", "B": "f", "D": 9},
    ])],
    [ForwardFillRule, SAMPLE_GROUPING_DF, ["C", "D"], ["A"], True, ["G", "H"], DataFrame(data=[
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 10, "G": 1, "H": "H1", "B": "a"},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 20, "G": 2, "H": "H1", "B": "f", "D": 9},
    ])],
    [ForwardFillRule, SAMPLE_GROUPING_DF, ["C", "D"], ["A"], False, ["G", "H"], DataFrame(data=[
        {"A": 20, "G": 2, "H": "H1", "B": "f"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 10, "G": 1, "H": "H1", "B": "a", "C": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
    ])],
    [BackFillRule, SAMPLE_DF, ["C", "D"], None, True, None, DataFrame(data=[
        {"A": 10, "B": "a",  "C": "e", "D": 9},
        {"A": 15,  "C": "e", "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 5},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e"},
        {"A": 20, "B": "f"},
    ])],
    [BackFillRule, SAMPLE_DF, ["C", "D"], ["A"], True, None, DataFrame(data=[
        {"A": 1, "B": "e", "C": "c", "D": 5},
        {"A": 5, "C": "c", "D": 5},
        {"A": 10, "B": "a", "C": "e", "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 9},
        {"A": 15, "D": 9},
        {"A": 20, "B": "f"},
    ])],
    [BackFillRule, SAMPLE_DF, ["C", "D"], ["A"], False, None, DataFrame(data=[
        {"A": 20, "B": "f", "C": "e", "D": 9},
        {"A": 15, "C": "e", "D": 9},
        {"A": 12, "B": "d", "C": "e", "D": 5},
        {"A": 10, "B": "a", "C": "c", "D": 5},
        {"A": 5, "C": "c", "D": 5},
        {"A": 1, "B": "e"},
    ])],
    [BackFillRule, SAMPLE_GROUPING_DF, ["C", "D"], None, True, ["G", "H"], DataFrame(data=[
        {"A": 10, "G": 1, "H": "H1", "B": "a", "C": "e"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 20, "G": 2, "H": "H1", "B": "f"},
    ])],
    [BackFillRule, SAMPLE_GROUPING_DF, ["C", "D"], ["A"], True, ["G", "H"], DataFrame(data=[
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 10, "G": 1, "H": "H1", "B": "a", "C": "e"},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 20, "G": 2, "H": "H1", "B": "f"},
    ])],
    [BackFillRule, SAMPLE_GROUPING_DF, ["C", "D"], ["A"], False, ["G", "H"], DataFrame(data=[
        {"A": 20, "G": 2, "H": "H1", "B": "f", "D": 9},
        {"A": 15, "G": 2, "H": "H1", "D": 9},
        {"A": 12, "G": 1, "H": "H1", "B": "d", "C": "e"},
        {"A": 10, "G": 1, "H": "H1", "B": "a"},
        {"A": 5, "G": 3, "H": "H1", "C": "c", "D": 5},
        {"A": 1, "G": 1, "H": "H2", "B": "e"},
    ])],
])
def test_forward_fill_rule_simple(rule_cls, input_sample, columns, sort_by, sort_ascending, group_by, expected):
    sample_df = input_sample.copy()
    sample_main_df = input_sample.copy()
    before_column_order = [col for col in sample_df.columns]
    data = RuleData(sample_main_df, named_inputs={"payload": sample_df})
    assert_frame_equal(data.get_main_output(), input_sample)
    assert_frame_equal(data.get_named_output("payload"), input_sample)
    rule = rule_cls(columns, sort_by, sort_ascending, group_by, named_input="payload", named_output="result")
    rule.apply(data)
    expected = expected[before_column_order]
    result = data.get_named_output("result")
    after_column_order = [col for col in result.columns]
    assert before_column_order == after_column_order
    assert_frame_equal(result, expected)
    # assert main output has not changed
    assert_frame_equal(data.get_main_output(), input_sample)
    assert_frame_equal(data.get_named_output("payload"), input_sample)


@pytest.mark.parametrize("rule_cls", [ForwardFillRule, BackFillRule])
def test_missing_sort_by_column(rule_cls):
    data = RuleData(SAMPLE_DF, named_inputs={"payload": SAMPLE_DF})
    rule = rule_cls(["C", "D"], sort_by=["E", "A"], named_input="payload", named_output="result")
    with pytest.raises(MissingColumn) as exc:
        rule.apply(data)
    assert str(exc.value) == "Missing sort_by column(s) in fill operation: {'E'}"


@pytest.mark.parametrize("rule_cls", [ForwardFillRule, BackFillRule])
def test_missing_group_by_column(rule_cls):
    data = RuleData(SAMPLE_DF, named_inputs={"payload": SAMPLE_DF})
    rule = rule_cls(["C", "D"], group_by=["E", "A"], named_input="payload", named_output="result")
    with pytest.raises(MissingColumn) as exc:
        rule.apply(data)
    assert str(exc.value) == "Missing group_by column(s) in fill operation: {'E'}"
