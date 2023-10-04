from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.exceptions import MissingColumnError
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
    {"A": "  AbCdEfG  ", "C": " cCcc", "D": -100},
    {"A": "babA   ", "C": "cCcc "},
    {"A": "  AAcAAA", "C": "cCcc", "D": -499},
    {"A": "diiI", "C": " cCcc  ", "D": 1},
])


INPUT_DF3 = DataFrame(data=[
    {"A": "AbCdEfG", "C": "cCcc", "D": -100},
    {"A": "babA", "C": "cCcc "},
    {"A": "AAcAAA", "C": " cCcc", "D": -499},
    {"A": "diiI", "C": " cCcc ", "D": 1},
])


INPUT_DF4 = DataFrame(data=[
    {"A": "A,B;C,D;E", "C": "cCcc", "D": -100},
    {"A": "1,2,3,4"},
    {"A": "1;2;3;4", "C": " cCcc", "D": -499},
    {"C": " cCcc ", "D": 1},
])


@pytest.mark.parametrize("rule_cls,columns,output_columns,input_df,expected", [
    [StrLowerRule, ["A", "C"], None, INPUT_DF, DataFrame(data=[
        {"A": "abcdefg", "B": 1.456, "C": "cccc", "D": -100},
        {"A": "baba", "B": -1.677, "C": "dddd"},
        {"A": "caaa", "B": 3.87, "D": -499},
        {"A": "diii", "B": -1.5, "C": "eeee", "D": 1},
    ])],
    [StrLowerRule, ["A", "C"], ["E", "F"], INPUT_DF, DataFrame(data=[
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "abcdefg", "F": "cccc"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "baba", "F": "dddd"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "caaa"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "diii", "F": "eeee"},
    ])],
    [StrLowerRule, ["A", "C", "Z"], None, INPUT_DF, MissingColumnError],
    [StrLowerRule, ["A", "C"], ["E"], INPUT_DF, ValueError],
    [StrUpperRule, ["A", "C"], None, INPUT_DF, DataFrame(data=[
        {"A": "ABCDEFG", "B": 1.456, "C": "CCCC", "D": -100},
        {"A": "BABA", "B": -1.677, "C": "DDDD"},
        {"A": "CAAA", "B": 3.87, "D": -499},
        {"A": "DIII", "B": -1.5, "C": "EEEE", "D": 1},
    ])],
    [StrUpperRule, ["A", "C"], ["E", "F"], INPUT_DF, DataFrame(data=[
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "ABCDEFG", "F": "CCCC"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "BABA", "F": "DDDD"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "CAAA"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "DIII", "F": "EEEE"},
    ])],
    [StrUpperRule, ["A", "C", "Z"], None, INPUT_DF, MissingColumnError],
    [StrUpperRule, ["A", "C"], ["E"], INPUT_DF, ValueError],
    [StrCapitalizeRule, ["A", "C"], None, INPUT_DF, DataFrame(data=[
        {"A": "Abcdefg", "B": 1.456, "C": "Cccc", "D": -100},
        {"A": "Baba", "B": -1.677, "C": "Dddd"},
        {"A": "Caaa", "B": 3.87, "D": -499},
        {"A": "Diii", "B": -1.5, "C": "Eeee", "D": 1},
    ])],
    [StrCapitalizeRule, ["A", "C"], ["E", "F"], INPUT_DF, DataFrame(data=[
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "Abcdefg", "F": "Cccc"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "Baba", "F": "Dddd"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "Caaa"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "Diii", "F": "Eeee"},
    ])],
    [StrCapitalizeRule, ["A", "C", "Z"], None, INPUT_DF, MissingColumnError],
    [StrCapitalizeRule, ["A", "C"], ["E"], INPUT_DF, ValueError],
])
def test_str_scenarios(rule_cls, columns, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(columns, output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("rule_cls,columns,how,characters,output_columns,input_df,expected", [
    [StrStripRule, ["A", "C"], "left", None, None, INPUT_DF2, DataFrame(data=[
        {"A": "AbCdEfG  ", "C": "cCcc", "D": -100},
        {"A": "babA   ", "C": "cCcc "},
        {"A": "AAcAAA", "C": "cCcc", "D": -499},
        {"A": "diiI", "C": "cCcc  ", "D": 1},
    ])],
    [StrStripRule, ["A", "C"], "right", None, None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG", "C": " cCcc", "D": -100},
        {"A": "babA", "C": "cCcc"},
        {"A": "  AAcAAA", "C": "cCcc", "D": -499},
        {"A": "diiI", "C": " cCcc", "D": 1},
    ])],
    [StrStripRule, ["A", "C"], "both", None, None, INPUT_DF2, DataFrame(data=[
        {"A": "AbCdEfG", "C": "cCcc", "D": -100},
        {"A": "babA", "C": "cCcc"},
        {"A": "AAcAAA", "C": "cCcc", "D": -499},
        {"A": "diiI", "C": "cCcc", "D": 1},
    ])],
    [StrStripRule, ["A", "C"], "left", "Ac", None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "C": " cCcc", "D": -100},
        {"A": "babA   ", "C": "Ccc "},
        {"A": "  AAcAAA", "C": "Ccc", "D": -499},
        {"A": "diiI", "C": " cCcc  ", "D": 1},
    ])],
    [StrStripRule, ["A", "C"], "right", "Ac", None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "C": " cC", "D": -100},
        {"A": "babA   ", "C": "cCcc "},
        {"A": "  ", "C": "cC", "D": -499},
        {"A": "diiI", "C": " cCcc  ", "D": 1},
    ])],
    [StrStripRule, ["A", "C"], "both", "Ac", None, INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "C": " cC", "D": -100},
        {"A": "babA   ", "C": "Ccc "},
        {"A": "  ", "C": "C", "D": -499},
        {"A": "diiI", "C": " cCcc  ", "D": 1},
    ])],
    [StrStripRule, ["A", "C"], "both", None, ["E", "F"], INPUT_DF2, DataFrame(data=[
        {"A": "  AbCdEfG  ", "C": " cCcc", "D": -100, "E": "AbCdEfG", "F": "cCcc"},
        {"A": "babA   ", "C": "cCcc ", "E": "babA", "F": "cCcc"},
        {"A": "  AAcAAA", "C": "cCcc", "D": -499, "E": "AAcAAA", "F": "cCcc"},
        {"A": "diiI", "C": " cCcc  ", "D": 1, "E": "diiI", "F": "cCcc"},
    ])],
    [StrStripRule, ["A", "C", "Z"], "left", None, None, INPUT_DF2, MissingColumnError],
    [StrStripRule, ["A", "C"], "both", None, ["E"], INPUT_DF2, ValueError],
])
def test_strip_scenarios(rule_cls, columns, how, characters, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = rule_cls(columns, how=how, characters=characters, output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("columns,width,fill_char,how,output_columns,input_df,expected", [
    [["A", "C"], 6, ".", "right", None, INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "C": "cCcc..", "D": -100},
        {"A": "babA..", "C": "cCcc ."},
        {"A": "AAcAAA", "C": " cCcc.", "D": -499},
        {"A": "diiI..", "C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], 6, ".", "left", None, INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "C": "..cCcc", "D": -100},
        {"A": "..babA", "C": ".cCcc "},
        {"A": "AAcAAA", "C": ". cCcc", "D": -499},
        {"A": "..diiI", "C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], 6, ".", "both", None, INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "C": ".cCcc.", "D": -100},
        {"A": ".babA.", "C": "cCcc ."},
        {"A": "AAcAAA", "C": " cCcc.", "D": -499},
        {"A": ".diiI.", "C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], 6, ".", "right", ["E", "F"], INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "C": "cCcc", "D": -100, "E": "AbCdEfG", "F": "cCcc.."},
        {"A": "babA", "C": "cCcc ", "E": "babA..", "F": "cCcc ."},
        {"A": "AAcAAA", "C": " cCcc", "D": -499, "E": "AAcAAA", "F": " cCcc."},
        {"A": "diiI", "C": " cCcc ", "D": 1, "E": "diiI..", "F": " cCcc "},
    ])],
    [["A", "C"], 6, ".", "left", ["E", "F"], INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "C": "cCcc", "D": -100, "E": "AbCdEfG", "F": "..cCcc"},
        {"A": "babA", "C": "cCcc ", "E": "..babA", "F": ".cCcc "},
        {"A": "AAcAAA", "C": " cCcc", "D": -499, "E": "AAcAAA", "F": ". cCcc"},
        {"A": "diiI", "C": " cCcc ", "D": 1, "E": "..diiI", "F": " cCcc "},
    ])],
    [["A", "C"], 6, ".", "both", ["E", "F"], INPUT_DF3, DataFrame(data=[
        {"A": "AbCdEfG", "C": "cCcc", "D": -100, "E": "AbCdEfG", "F": ".cCcc."},
        {"A": "babA", "C": "cCcc ", "E": ".babA.", "F": "cCcc ."},
        {"A": "AAcAAA", "C": " cCcc", "D": -499, "E": "AAcAAA", "F": " cCcc."},
        {"A": "diiI", "C": " cCcc ", "D": 1, "E": ".diiI.", "F": " cCcc "},
    ])],
    [["A", "C", "Z"], 6, ".", "left", None, INPUT_DF3, MissingColumnError],
    [["A", "C"], 6, ".", "both", ["E"], INPUT_DF3, ValueError],
])
def test_pad_scenarios(columns, width, fill_char, how, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = StrPadRule(columns, width=width, fill_character=fill_char, how=how, output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("columns,separator,separator_regex,limit,output_columns,input_df,expected", [
    [["A", "C"], ",", None, None, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B;C", "D;E"], "C": ["cCcc"], "D": -100},
        {"A": ["1", "2", "3", "4"]},
        {"A": ["1;2;3;4"], "C": [" cCcc"], "D": -499},
        {"C": [" cCcc "], "D": 1},
    ])],
    [["A", "C"], ",", None, 2, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B;C", "D;E"], "C": ["cCcc"], "D": -100},
        {"A": ["1", "2", "3,4"]},
        {"A": ["1;2;3;4"], "C": [" cCcc"], "D": -499},
        {"C": [" cCcc "], "D": 1},
    ])],
    [["A", "C"], ";", None, None, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A,B", "C,D", "E"], "C": ["cCcc"], "D": -100},
        {"A": ["1,2,3,4"]},
        {"A": ["1", "2", "3", "4"], "C": [" cCcc"], "D": -499},
        {"C": [" cCcc "], "D": 1},
    ])],
    [["A", "C"], None, ",|;", None, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B", "C", "D", "E"], "C": ["cCcc"], "D": -100},
        {"A": ["1", "2", "3", "4"]},
        {"A": ["1", "2", "3", "4"], "C": [" cCcc"], "D": -499},
        {"C": [" cCcc "], "D": 1},
    ])],
    [["A", "C"], None, ",|;", 2, None, INPUT_DF4, DataFrame(data=[
        {"A": ["A", "B", "C,D;E"], "C": ["cCcc"], "D": -100},
        {"A": ["1", "2", "3,4"]},
        {"A": ["1", "2", "3;4"], "C": [" cCcc"], "D": -499},
        {"C": [" cCcc "], "D": 1},
    ])],
    [["A", "C"], ",", None, None, ["E", "F"], INPUT_DF4, DataFrame(data=[
        {"A": "A,B;C,D;E", "C": "cCcc", "D": -100, "E": ["A", "B;C", "D;E"], "F": ["cCcc"]},
        {"A": "1,2,3,4", "E": ["1", "2", "3", "4"]},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499, "E": ["1;2;3;4"], "F": [" cCcc"]},
        {"C": " cCcc ", "D": 1, "F": [" cCcc "]},
    ])],
    [["A", "C", "Z"], ",", None, None, None, INPUT_DF4, MissingColumnError],
    [["A", "C"], ",", None, None, ["E"], INPUT_DF4, ValueError],
])
def test_split_scenarios(columns, separator, separator_regex, limit, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = StrSplitRule(columns, separator=separator, separator_regex=separator_regex, limit=limit, output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("columns,separator,separator_regex,limit,new_separator,sort,output_columns,input_df,expected", [
    [["A", "C"], ",", None, None, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], ",", None, None, "|", "ascending", None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], ",", None, None, "|", "descending", None, INPUT_DF4, DataFrame(data=[
        {"A": "D;E|B;C|A", "C": "cCcc", "D": -100},
        {"A": "4|3|2|1"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], ",", None, 2, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3,4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], ";", None, None, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A,B|C,D|E", "C": "cCcc", "D": -100},
        {"A": "1,2,3,4"},
        {"A": "1|2|3|4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], None, ",|;", None, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B|C|D|E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1|2|3|4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], None, ",|;", 2, "|", None, None, INPUT_DF4, DataFrame(data=[
        {"A": "A|B|C,D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3,4"},
        {"A": "1|2|3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ])],
    [["A", "C"], ",", None, None, "|", None, ["E", "F"], INPUT_DF4, DataFrame(data=[
        {"A": "A,B;C,D;E", "C": "cCcc", "D": -100, "E": "A|B;C|D;E", "F": "cCcc"},
        {"A": "1,2,3,4", "E": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499, "E": "1;2;3;4", "F": " cCcc"},
        {"C": " cCcc ", "D": 1, "F": " cCcc "},
    ])],
    [["A", "C", "Z"], ",", None, None, "|", None, None, INPUT_DF4, MissingColumnError],
    [["A", "C"], ",", None, None, "|", None, ["E"], INPUT_DF4, ValueError],
])
def test_split_rejoin_scenarios(columns, separator, separator_regex, limit, new_separator, sort, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = StrSplitRejoinRule(
            columns, separator=separator, separator_regex=separator_regex, limit=limit, new_separator=new_separator,
            sort=sort, output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False



@pytest.mark.parametrize("columns,regular_expression,keep_original_value,output_columns,input_df,expected", [
    [["A"], r"a([\d]*)_end", True, None, 
        DataFrame(data=[{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
        DataFrame(data=[{"A": "123", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
    ],
    [["A"], r"a([\d]*)_end", False, None, 
        DataFrame(data=[{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
        DataFrame(data=[{"A": "123", "B": "a321_end"}, {"B": "a321f_end"}]),
    ],
    [["A", "B"], r"a([\d]*)_end", True, None, 
        DataFrame(data=[{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
        DataFrame(data=[{"A": "123", "B": "321"}, {"A": "a123f_end", "B": "a321f_end"}]),
    ],
    [["A", "B"], r"a([\d]*)_end", False, None, 
        DataFrame(data=[{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}]),
        DataFrame(data=[{"A": "123", "B": "321"}, {}]),
    ],
    [["A"], r"a([\d]*)((?:f{0,1})_end)", True, ["E", "F"], 
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
    [["A"], r"a([\d]*)((?:f{0,1})_end)", False, ["E", "F"], 
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
    [["A", "B"], r"a([\d]*)((?:f{0,1})_end)", True, ["E", "F", "G", "H"], 
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end"},
            {"A": "a123f_end", "B": "a321f_end"},
            {"A": "no_match", "B": "nomatch"},
        ]),
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end", "E": "123", "F": "_end", "G": "321", "H": "_end"},
            {"A": "a123f_end", "B": "a321f_end", "E": "123", "F": "f_end", "G": "321", "H": "f_end"},
            {"A": "no_match", "B": "nomatch", "E": "no_match", "G": "nomatch"},
        ]),
    ],
    [["A", "B"], r"a([\d]*)((?:f{0,1})_end)", False, ["E", "F", "G", "H"], 
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end"},
            {"A": "a123f_end", "B": "a321f_end"},
            {"A": "no_match", "B": "nomatch"},
        ]),
        DataFrame(data=[
            {"A": "a123_end", "B": "a321_end", "E": "123", "F": "_end", "G": "321", "H": "_end"},
            {"A": "a123f_end", "B": "a321f_end", "E": "123", "F": "f_end", "G": "321", "H": "f_end"},
            {"A": "no_match", "B": "nomatch"},
        ]),
    ],
    [["A", "C", "Z"], "a(.*)", True, None, INPUT_DF4, MissingColumnError],
])
def test_extract_scenarios(columns, regular_expression, keep_original_value, output_columns, input_df, expected):
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = StrExtractRule(
            columns, regular_expression=regular_expression, keep_original_value=keep_original_value,
            output_columns=output_columns, named_input="input", named_output="result")
        if isinstance(expected, DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False