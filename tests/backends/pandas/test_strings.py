from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.backends.pandas import (
    StrLowerRule, StrUpperRule, StrCapitalizeRule, StrStripRule, StrPadRule,
    StrSplitRule, StrSplitRejoinRule, StrExtractRule
)
from tests.backends.pandas.utils.data import get_test_data


INPUT_DF = DataFrame(data=[
    {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100},
    {"A": "babA", "B": -1.677, "C": "dDdd"},
    {"A": "cAAA", "B": 3.87, "D": -499},
    {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1},
])


INPUT_DF2 = DataFrame(data=[
    {"A": "  AbCdEfG  ", "D": -100},
    {"A": "babA   "},
    {"A": "  AAcAAA", "D": -499},
    {"A": "diiI", "D": 1},
    {},
])


INPUT_DF3 = DataFrame(data=[
    {"A": "AbCdEfG", "D": -100},
    {"A": "babA"},
    {"A": "AAcAAA", "D": -499},
    {"A": "diiI", "D": 1},
    {},
])


@pytest.mark.parametrize("rule_cls,input_column,output_column,input_df,expected", [
    [StrLowerRule, "A", None, DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": []}, dtype="string")],
    [StrLowerRule, "A", None, INPUT_DF, DataFrame(data=[
        {"A": "abcdefg", "B": 1.456, "C": "cCcc", "D": -100},
        {"A": "baba", "B": -1.677, "C": "dDdd"},
        {"A": "caaa", "B": 3.87, "D": -499},
        {"A": "diii", "B": -1.5, "C": "eEee", "D": 1},
    ])],
    [StrLowerRule, "A", "E", INPUT_DF, DataFrame(data=[
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "abcdefg"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "baba"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "caaa"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "diii"},
    ])],
    [StrLowerRule, "Z", None, INPUT_DF, MissingColumnError],
    [StrLowerRule, "A", "A", INPUT_DF, ColumnAlreadyExistsError],
    [StrUpperRule, "A", None, INPUT_DF, DataFrame(data=[
        {"A": "ABCDEFG", "B": 1.456, "C": "cCcc", "D": -100},
        {"A": "BABA", "B": -1.677, "C": "dDdd"},
        {"A": "CAAA", "B": 3.87, "D": -499},
        {"A": "DIII", "B": -1.5, "C": "eEee", "D": 1},
    ])],
    [StrUpperRule, "A", None, DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": []}, dtype="string")],
    [StrUpperRule, "A", "E", INPUT_DF, DataFrame(data=[
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "ABCDEFG"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "BABA"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "CAAA"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "DIII"},
    ])],
    [StrUpperRule, "Z", None, INPUT_DF, MissingColumnError],
    [StrUpperRule, "A", "A", INPUT_DF, ColumnAlreadyExistsError],
    [StrCapitalizeRule, "A", None, DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": []}, dtype="string")],
    [StrCapitalizeRule, "A", None, INPUT_DF, DataFrame(data=[
        {"A": "Abcdefg", "B": 1.456, "C": "cCcc", "D": -100},
        {"A": "Baba", "B": -1.677, "C": "dDdd"},
        {"A": "Caaa", "B": 3.87, "D": -499},
        {"A": "Diii", "B": -1.5, "C": "eEee", "D": 1},
    ])],
    [StrCapitalizeRule, "A", "E", INPUT_DF, DataFrame(data=[
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "Abcdefg"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "Baba"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "Caaa"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "Diii"},
    ])],
    [StrCapitalizeRule, "Z", None, INPUT_DF, MissingColumnError],
    [StrCapitalizeRule, "A", "A", INPUT_DF, ColumnAlreadyExistsError],
])
def test_str_scenarios(rule_cls, input_column, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(input_column, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("rule_cls,input_column,how,characters,output_column,input_df,expected", [
    [StrStripRule, "A", "left", None, None, INPUT_DF2, DataFrame(data=[
        {"A": "AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ])],
    [StrStripRule, "A", "right", None, None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG", "D": -100},
        {"A": "babA"},
        {"A": "  AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ])],
    [StrStripRule, "A", "both", None, None, INPUT_DF2, DataFrame(data=[
        {"A": "AbCdEfG", "D": -100},
        {"A": "babA"},
        {"A": "AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ])],
    [StrStripRule, "A", "left", "Ac", None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "  AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ])],
    [StrStripRule, "A", "right", "Ac", None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "  ", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ])],
    [StrStripRule, "A", "both", "Ac", None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "  ", "D": -499},
        {"A": "diiI","D": 1},
        {},
    ])],
    [StrStripRule, "A", "both", None, "E", INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "D": -100, "E": "AbCdEfG"},
        {"A": "babA   ", "E": "babA"},
        {"A": "  AAcAAA", "D": -499, "E": "AAcAAA"},
        {"A": "diiI", "D": 1, "E": "diiI"},
        {},
    ])],
    [StrStripRule, "A", "left", None, None, DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": []}, dtype="string")],
    [StrStripRule, "Z", "left", None, None, INPUT_DF2, MissingColumnError],
    [StrStripRule, "A", "both", None, "D", INPUT_DF2, ColumnAlreadyExistsError],
])
def test_strip_scenarios(rule_cls, input_column, how, characters, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(input_column, how=how, characters=characters, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("input_column,width,fill_char,how,output_column,input_df,expected", [
    ["A", 6, ".", "right", None, INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "D": -100},
        {"A": "babA.."},
        {"A": "AAcAAA", "D": -499},
        {"A": "diiI..", "D": 1},
        {},
    ])],
    ["A", 6, ".", "left", None, INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "D": -100},
        {"A": "..babA"},
        {"A": "AAcAAA", "D": -499},
        {"A": "..diiI",  "D": 1},
        {},
    ])],
    ["A", 6, ".", "both", None, INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "D": -100},
        {"A": ".babA."},
        {"A": "AAcAAA", "D": -499},
        {"A": ".diiI.", "D": 1},
        {},
    ])],
    ["A", 6, ".", "right", "E", INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "D": -100, "E": "AbCdEfG"},
        {"A": "babA", "E": "babA.."},
        {"A": "AAcAAA", "D": -499, "E": "AAcAAA"},
        {"A": "diiI", "D": 1, "E": "diiI.."},
        {},
    ])],
    ["A", 6, ".", "left", "E", INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "D": -100, "E": "AbCdEfG"},
        {"A": "babA", "E": "..babA"},
        {"A": "AAcAAA", "D": -499, "E": "AAcAAA"},
        {"A": "diiI", "D": 1, "E": "..diiI"},
        {},
    ])],
    ["A", 6, ".", "both", "E", INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "D": -100, "E": "AbCdEfG"},
        {"A": "babA", "E": ".babA."},
        {"A": "AAcAAA", "D": -499, "E": "AAcAAA"},
        {"A": "diiI", "D": 1, "E": ".diiI."},
        {},
    ])],
    ["A", 6, ".", "both", None, DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": []}, dtype="string")],
    ["Z", 6, ".", "left", None, INPUT_DF3, MissingColumnError],
    ["A", 6, ".", "both", "D", INPUT_DF3, ColumnAlreadyExistsError],
])
def test_pad_scenarios(input_column, width, fill_char, how, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = StrPadRule(input_column, width=width, fill_character=fill_char, how=how, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_DF4 = DataFrame(data=[
    {"A": "A,B;C,D;E", "C": "cCcc", "D": -100},
    {"A": "1,2,3,4"},
    {"A": "1;2;3;4", "C": " cCcc", "D": -499},
    {"C": " cCcc ", "D": 1},
])

@pytest.mark.parametrize("input_column,separator,separator_regex,limit,output_column,input_df,expected", [
    ["A", ",", None, None, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B;C", "D;E"], "C": "cCcc", "D": -100},
        {"A": ["1", "2", "3", "4"]},
        {"A": ["1;2;3;4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    ["A", ",", None, 2, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B;C", "D;E"], "C": "cCcc", "D": -100},
        {"A": ["1", "2", "3,4"]},
        {"A": ["1;2;3;4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    ["A", ";", None, None, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A,B", "C,D", "E"], "C": "cCcc", "D": -100},
        {"A": ["1,2,3,4"]},
        {"A": ["1", "2", "3", "4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    ["A", None, ",|;", None, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B", "C", "D", "E"], "C": "cCcc", "D": -100},
        {"A": ["1", "2", "3", "4"]},
        {"A": ["1", "2", "3", "4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    ["A", None, ",|;", 2, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B", "C,D;E"], "C": "cCcc", "D": -100},
        {"A": ["1", "2", "3,4"]},
        {"A": ["1", "2", "3;4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    ["A", ",", None, None, "E", INPUT_DF4, DataFrame(data=[
        {"A": "A,B;C,D;E", "C": "cCcc", "D": -100, "E": ["A", "B;C", "D;E"]},
        {"A": "1,2,3,4", "E": ["1", "2", "3", "4"]},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499, "E": ["1;2;3;4"]},
        {"C": " cCcc ", "D": 1},
    ])],
    ["A", ".", None, None, None, DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": []}, dtype="object")],
    ["Z", ",", None, None, None, INPUT_DF4, MissingColumnError],
    ["A", ",", None, None, "C", INPUT_DF4, ColumnAlreadyExistsError],
])
def test_split_scenarios(input_column, separator, separator_regex, limit, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = StrSplitRule(input_column, separator=separator, separator_regex=separator_regex, limit=limit, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("input_column,separator,separator_regex,limit,new_separator,sort,output_column,input_df,expected", [
    ["A", ",", None, None, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ]).astype({"A": "string"})],
    ["A", ",", None, None, "|", "ascending", None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ]).astype({"A": "string"})],
    ["A", ",", None, None, "|", "descending", None, INPUT_DF4, DataFrame(data=[
        {"A": "D;E|B;C|A", "C": "cCcc", "D": -100},
        {"A": "4|3|2|1"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ]).astype({"A": "string"})],
    ["A", ",", None, 2, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3,4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ]).astype({"A": "string"})],
    ["A", ";", None, None, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A,B|C,D|E", "C": "cCcc", "D": -100},
        {"A": "1,2,3,4"},
        {"A": "1|2|3|4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ]).astype({"A": "string"})],
    ["A", None, ",|;", None, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B|C|D|E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1|2|3|4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ]).astype({"A": "string"})],
    ["A", None, ",|;", 2, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B|C,D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3,4"},
        {"A": "1|2|3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ]).astype({"A": "string"})],
    ["A", ",", None, None, "|", None, "E", INPUT_DF4, DataFrame(data=[
        {"A": "A,B;C,D;E", "C": "cCcc", "D": -100, "E": "A|B;C|D;E"},
        {"A": "1,2,3,4", "E": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499, "E": "1;2;3;4"},
        {"C": " cCcc ", "D": 1},
    ]).astype({"E": "string"})],
    ["A", ".", None, None, "|", None, None, DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": []}, dtype="string")],
    ["Z", ",", None, None, "|", None, None, INPUT_DF4, MissingColumnError],
    ["A", ",", None, None, "|", None, "C", INPUT_DF4, ColumnAlreadyExistsError],
])
def test_split_rejoin_scenarios(input_column, separator, separator_regex, limit, new_separator, sort, output_column, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = StrSplitRejoinRule(
            input_column, separator=separator, separator_regex=separator_regex, limit=limit, new_separator=new_separator,
            sort=sort, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("input_column,regular_expression,keep_original_value,output_columns,input_df,expected", [
    ["A", r"a([\d]*)_end", True, None, 
        DataFrame(data=[{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
        DataFrame(data=[{"A": "123", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
    ],
    ["A", r"a([\d]*)_end", False, None, 
        DataFrame(data=[{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
        DataFrame(data=[{"A": "123", "B": "a321_end"}, {"B": "a321f_end"}]),
    ],
    ["A", r"a([\d]*)((?:f{0,1})_end)", True, ["E", "F"], 
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end"},
            {"A": "a123f_end", "B": "a321f_end"},
            {"A": "no_match", "B": "a321f_end"},
        ]),
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end", "E": "123", "F": "_end"},
            {"A": "a123f_end", "B": "a321f_end", "E": "123", "F": "f_end"},
            {"A": "no_match", "B": "a321f_end", "E": "no_match"},
        ]),
    ],
    ["A", r"a([\d]*)((?:f{0,1})_end)", False, ["E", "F"], 
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end"},
            {"A": "a123f_end", "B": "a321f_end"},
            {"A": "no_match", "B": "a321f_end"},
        ]),
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end", "E": "123", "F": "_end"},
            {"A": "a123f_end", "B": "a321f_end", "E": "123", "F": "f_end"},
            {"A": "no_match", "B": "a321f_end"},
        ]),
    ],
    ["A", r"a([\d]*)((?:f{0,1})_end)", False, ["E", "F"], DataFrame(data={"A": []}, dtype="string"), DataFrame(data={"A": [], "E": [], "F": []}, dtype="string")],
    ["Z", "a(.*)", True, ["C"], INPUT_DF4, MissingColumnError],
    ["A", "a(.*)", True, ["C"], INPUT_DF4, ColumnAlreadyExistsError],
    ["A", "a(.*)-([0-9]*)", True, None, INPUT_DF4, ValueError],
    ["A", "a(.*)-([0-9]*)", True, ["E"], INPUT_DF4, ValueError],
])
def test_extract_scenarios(input_column, regular_expression, keep_original_value, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, DataFrame):
            rule = StrExtractRule(
                input_column, regular_expression=regular_expression, keep_original_value=keep_original_value,
                output_columns=output_columns, named_input="input", named_output="result")
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule = StrExtractRule(
                    input_column, regular_expression=regular_expression, keep_original_value=keep_original_value,
                    output_columns=output_columns, named_input="input", named_output="result")
                rule.apply(data)
        else:
            assert False