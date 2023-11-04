from pandas import DataFrame
import pytest

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.backends.pandas import (
    StrLowerRule, StrUpperRule, StrCapitalizeRule, StrStripRule, StrPadRule,
    StrSplitRule, StrSplitRejoinRule, StrExtractRule
)
from tests.utils.data import assert_frame_equal, get_test_data


INPUT_DF = [
    {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100},
    {"A": "babA", "B": -1.677, "C": "dDdd"},
    {"A": "cAAA", "B": 3.87, "D": -499},
    {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1},
]


INPUT_DF2 = [
    {"A": "  AbCdEfG  ", "D": -100},
    {"A": "babA   "},
    {"A": "  AAcAAA", "D": -499},
    {"A": "diiI", "D": 1},
    {},
]


INPUT_DF3 = [
    {"A": "AbCdEfG", "D": -100},
    {"A": "babA"},
    {"A": "AAcAAA", "D": -499},
    {"A": "diiI", "D": 1},
    {},
]


@pytest.mark.parametrize("rule_cls_str,input_column,output_column,input_df,input_dtype,expected,expected_dtype", [
    ["StrLowerRule", "A", None, {"A": []}, "string", {"A": []}, "string"],
    ["StrLowerRule", "A", None, INPUT_DF, None, [
        {"A": "abcdefg", "B": 1.456, "C": "cCcc", "D": -100},
        {"A": "baba", "B": -1.677, "C": "dDdd"},
        {"A": "caaa", "B": 3.87, "D": -499},
        {"A": "diii", "B": -1.5, "C": "eEee", "D": 1},
    ], None],
    ["StrLowerRule", "A", "E", INPUT_DF, None, [
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "abcdefg"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "baba"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "caaa"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "diii"},
    ], None],
    ["StrLowerRule", "Z", None, INPUT_DF, None, MissingColumnError, None],
    ["StrLowerRule", "A", "A", INPUT_DF, None, ColumnAlreadyExistsError, None],
    ["StrUpperRule", "A", None, INPUT_DF, None, [
        {"A": "ABCDEFG", "B": 1.456, "C": "cCcc", "D": -100},
        {"A": "BABA", "B": -1.677, "C": "dDdd"},
        {"A": "CAAA", "B": 3.87, "D": -499},
        {"A": "DIII", "B": -1.5, "C": "eEee", "D": 1},
    ], None],
    ["StrUpperRule", "A", None, {"A": []}, "string", {"A": []}, "string"],
    ["StrUpperRule", "A", "E", INPUT_DF, None, [
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "ABCDEFG"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "BABA"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "CAAA"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "DIII"},
    ], None],
    ["StrUpperRule", "Z", None, INPUT_DF, None, MissingColumnError, None],
    ["StrUpperRule", "A", "A", INPUT_DF, None, ColumnAlreadyExistsError, None],
    ["StrCapitalizeRule", "A", None, {"A": []}, "string", {"A": []}, "string"],
    ["StrCapitalizeRule", "A", None, INPUT_DF, None, [
        {"A": "Abcdefg", "B": 1.456, "C": "cCcc", "D": -100},
        {"A": "Baba", "B": -1.677, "C": "dDdd"},
        {"A": "Caaa", "B": 3.87, "D": -499},
        {"A": "Diii", "B": -1.5, "C": "eEee", "D": 1},
    ], None],
    ["StrCapitalizeRule", "A", "E", INPUT_DF, None, [
        {"A": "AbCdEfG", "B": 1.456, "C": "cCcc", "D": -100, "E": "Abcdefg"},
        {"A": "babA", "B": -1.677, "C": "dDdd", "E": "Baba"},
        {"A": "cAAA", "B": 3.87, "D": -499, "E": "Caaa"},
        {"A": "diiI", "B": -1.5, "C": "eEee", "D": 1, "E": "Diii"},
    ], None],
    ["StrCapitalizeRule", "Z", None, INPUT_DF, None, MissingColumnError, None],
    ["StrCapitalizeRule", "A", "A", INPUT_DF, None, ColumnAlreadyExistsError, None],
])
def test_str_scenarios(rule_cls_str, input_column, output_column, input_df, input_dtype, expected, expected_dtype, backend):
    input_df = backend.DataFrame(input_df, dtype=input_dtype)
    expected = backend.DataFrame(expected, dtype=expected_dtype) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule_cls = getattr(backend.rules, rule_cls_str)
        rule = rule_cls(input_column, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("rule_cls_str,input_column,how,characters,output_column,input_df,input_dtype,expected,expected_dtype", [
    ["StrStripRule", "A", "left", None, None, INPUT_DF2, None, [
        {"A": "AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ], None],
    ["StrStripRule", "A", "right", None, None, INPUT_DF2, None, [
        {"A": "  AbCdEfG", "D": -100},
        {"A": "babA"},
        {"A": "  AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ], None],
    ["StrStripRule", "A", "both", None, None, INPUT_DF2, None, [
        {"A": "AbCdEfG", "D": -100},
        {"A": "babA"},
        {"A": "AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ], None],
    ["StrStripRule", "A", "left", "Ac", None, INPUT_DF2, None, [
        {"A": "  AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "  AAcAAA", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ], None],
    ["StrStripRule", "A", "right", "Ac", None, INPUT_DF2, None, [
        {"A": "  AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "  ", "D": -499},
        {"A": "diiI", "D": 1},
        {},
    ], None],
    ["StrStripRule", "A", "both", "Ac", None, INPUT_DF2, None, [
        {"A": "  AbCdEfG  ", "D": -100},
        {"A": "babA   "},
        {"A": "  ", "D": -499},
        {"A": "diiI","D": 1},
        {},
    ], None],
    ["StrStripRule", "A", "both", None, "E", INPUT_DF2, None, [
        {"A": "  AbCdEfG  ", "D": -100, "E": "AbCdEfG"},
        {"A": "babA   ", "E": "babA"},
        {"A": "  AAcAAA", "D": -499, "E": "AAcAAA"},
        {"A": "diiI", "D": 1, "E": "diiI"},
        {},
    ], None],
    ["StrStripRule", "A", "left", None, None, {"A": []}, "string", {"A": []}, "string"],
    ["StrStripRule", "Z", "left", None, None, INPUT_DF2, None, MissingColumnError, None],
    ["StrStripRule", "A", "both", None, "D", INPUT_DF2, None, ColumnAlreadyExistsError, None],
])
def test_strip_scenarios(rule_cls_str, input_column, how, characters, output_column, input_df, input_dtype, expected, expected_dtype, backend):
    input_df = backend.DataFrame(data=input_df, dtype=input_dtype)
    expected = backend.DataFrame(data=expected, dtype=expected_dtype) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule_cls = getattr(backend.rules, rule_cls_str)
        rule = rule_cls(input_column, how=how, characters=characters, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("input_column,width,fill_char,how,output_column,input_df,input_dtype,expected,expected_dtype", [
    ["A", 6, ".", "right", None, INPUT_DF3, None, [
        {"A": "AbCdEfG", "D": -100},
        {"A": "babA.."},
        {"A": "AAcAAA", "D": -499},
        {"A": "diiI..", "D": 1},
        {},
    ], None],
    ["A", 6, ".", "left", None, INPUT_DF3, None, [
        {"A": "AbCdEfG", "D": -100},
        {"A": "..babA"},
        {"A": "AAcAAA", "D": -499},
        {"A": "..diiI",  "D": 1},
        {},
    ], None],
    ["A", 6, ".", "right", "E", INPUT_DF3, None, [
        {"A": "AbCdEfG", "D": -100, "E": "AbCdEfG"},
        {"A": "babA", "E": "babA.."},
        {"A": "AAcAAA", "D": -499, "E": "AAcAAA"},
        {"A": "diiI", "D": 1, "E": "diiI.."},
        {},
    ], None],
    ["A", 6, ".", "left", "E", INPUT_DF3, None, [
        {"A": "AbCdEfG", "D": -100, "E": "AbCdEfG"},
        {"A": "babA", "E": "..babA"},
        {"A": "AAcAAA", "D": -499, "E": "AAcAAA"},
        {"A": "diiI", "D": 1, "E": "..diiI"},
        {},
    ], None],
    ["A", 6, ".", "left", None, {"A": []}, "string", {"A": []}, "string"],
    ["Z", 6, ".", "left", None, INPUT_DF3, None, MissingColumnError, None],
    ["A", 6, ".", "right", "D", INPUT_DF3, None, ColumnAlreadyExistsError, None],
])
def test_pad_scenarios(input_column, width, fill_char, how, output_column, input_df, input_dtype, expected, expected_dtype, backend):
    input_df = backend.DataFrame(data=input_df, dtype=input_dtype)
    expected = backend.DataFrame(data=expected, dtype=expected_dtype) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.StrPadRule(input_column, width=width, fill_character=fill_char, how=how, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


INPUT_DF4 = [
    {"A": "A,B;C,D;E", "C": "cCcc", "D": -100},
    {"A": "1,2,3,4"},
    {"A": "1;2;3;4", "C": " cCcc", "D": -499},
    {"C": " cCcc ", "D": 1},
]

@pytest.mark.parametrize("input_column,separator,limit,output_column,input_df,input_dtype,expected,expected_dtype", [
    ["A", ",", None, None, INPUT_DF4, None, [
        {"A": ["A", "B;C", "D;E"], "C": "cCcc", "D": -100},
        {"A": ["1", "2", "3", "4"]},
        {"A": ["1;2;3;4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None],
    ["A", ",", 2, None, INPUT_DF4, None, [
        {"A": ["A", "B;C", "D;E"], "C": "cCcc", "D": -100},
        {"A": ["1", "2", "3,4"]},
        {"A": ["1;2;3;4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None],
    ["A", ";", None, None, INPUT_DF4, None, [
        {"A": ["A,B", "C,D", "E"], "C": "cCcc", "D": -100},
        {"A": ["1,2,3,4"]},
        {"A": ["1", "2", "3", "4"], "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None],
    ["A", ",", None, "E", INPUT_DF4, None, [
        {"A": "A,B;C,D;E", "C": "cCcc", "D": -100, "E": ["A", "B;C", "D;E"]},
        {"A": "1,2,3,4", "E": ["1", "2", "3", "4"]},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499, "E": ["1;2;3;4"]},
        {"C": " cCcc ", "D": 1},
    ], None],
    ["A", ".", None, None, {"A": []}, "string", {"A": []}, "list_strings"],
    ["Z", ",", None, None, INPUT_DF4, None, MissingColumnError, None],
    ["A", ",", None, "C", INPUT_DF4, None, ColumnAlreadyExistsError, None],
])
def test_split_scenarios(input_column, separator, limit, output_column, input_df, input_dtype, expected, expected_dtype, backend):
    input_df = backend.DataFrame(data=input_df, dtype=input_dtype)
    expected = backend.DataFrame(data=expected, dtype=expected_dtype) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.StrSplitRule(input_column, separator=separator, limit=limit, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("input_column,separator,limit,new_separator,sort,output_column,input_df,input_dtype,expected,expected_dtype,expected_astype", [
    ["A", ",", None, "|", None, None, INPUT_DF4, None, [
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None, {"A": "string"}],
    ["A", ",", None, "|", "ascending", None, INPUT_DF4, None, [
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None, {"A": "string"}],
    ["A", ",", None, "|", "descending", None, INPUT_DF4, None, [
        {"A": "D;E|B;C|A", "C": "cCcc", "D": -100},
        {"A": "4|3|2|1"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None, {"A": "string"}],
    ["A", ",", 2, "|", None, None, INPUT_DF4, None, [
        {"A": "A|B;C|D;E", "C": "cCcc", "D": -100},
        {"A": "1|2|3,4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None, {"A": "string"}],
    ["A", ";", None, "|", None, None, INPUT_DF4, None, [
        {"A": "A,B|C,D|E", "C": "cCcc", "D": -100},
        {"A": "1,2,3,4"},
        {"A": "1|2|3|4", "C": " cCcc", "D": -499},
        {"C": " cCcc ", "D": 1},
    ], None, {"A": "string"}],
    ["A", ",", None, "|", None, "E", INPUT_DF4, None, [
        {"A": "A,B;C,D;E", "C": "cCcc", "D": -100, "E": "A|B;C|D;E"},
        {"A": "1,2,3,4", "E": "1|2|3|4"},
        {"A": "1;2;3;4", "C": " cCcc", "D": -499, "E": "1;2;3;4"},
        {"C": " cCcc ", "D": 1},
    ], None, {"E": "string"}],
    ["A", ".", None, "|", None, None, {"A": []}, "string", {"A": []}, "string", None],
    ["Z", ",", None, "|", None, None, INPUT_DF4, None, MissingColumnError, None, None],
    ["A", ",", None, "|", None, "C", INPUT_DF4, None, ColumnAlreadyExistsError, None, None],
])
def test_split_rejoin_scenarios(input_column, separator, limit, new_separator, sort, output_column, input_df, input_dtype, expected, expected_dtype, expected_astype, backend):
    input_df = backend.DataFrame(data=input_df, dtype=input_dtype)
    expected = backend.DataFrame(data=expected, dtype=expected_dtype, astype=expected_astype) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.StrSplitRejoinRule(
            input_column, separator=separator, limit=limit, new_separator=new_separator,
            sort=sort, output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("input_column,regular_expression,keep_original_value,output_columns,input_df,input_dtype,expected,expected_dtype", [
    ["A", r"a([\d]*)_end", True, None, 
        [{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}], None,
        [{"A": "123", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}], None,
    ],
    ["A", r"a([\d]*)_end", False, None, 
        [{"A": "a123_end", "B": "a321_end"}, {"A": "a123f_end", "B": "a321f_end"}], None,
        [{"A": "123", "B": "a321_end"}, {"B": "a321f_end"}], None,
    ],
    ["A", r"a([\d]*)((?:f{0,1})_end)", True, ["E", "F"], 
        [
            {"A": "a123_end", "B": "a321_end"},
            {"A": "a123f_end", "B": "a321f_end"},
            {"A": "no_match", "B": "a321f_end"},
        ], None,
        [
            {"A": "a123_end", "B": "a321_end", "E": "123", "F": "_end"},
            {"A": "a123f_end", "B": "a321f_end", "E": "123", "F": "f_end"},
            {"A": "no_match", "B": "a321f_end", "E": "no_match"},
        ], None,
    ],
    ["A", r"a([\d]*)((?:f{0,1})_end)", False, ["E", "F"], 
        [
            {"A": "a123_end", "B": "a321_end"},
            {"A": "a123f_end", "B": "a321f_end"},
            {"A": "no_match", "B": "a321f_end"},
        ], None,
        [
            {"A": "a123_end", "B": "a321_end", "E": "123", "F": "_end"},
            {"A": "a123f_end", "B": "a321f_end", "E": "123", "F": "f_end"},
            {"A": "no_match", "B": "a321f_end"},
        ], None,
    ],
    ["A", r"a([\d]*)((?:f{0,1})_end)", False, ["E", "F"], {"A": []}, "string", {"A": [], "E": [], "F": []}, "string"],
    ["Z", "a(.*)", True, ["C"], INPUT_DF4, None, MissingColumnError, None],
    ["A", "a(.*)", True, ["C"], INPUT_DF4, None, ColumnAlreadyExistsError, None],
    ["A", "a(.*)-([0-9]*)", True, None, INPUT_DF4, None, ValueError, None],
    ["A", "a(.*)-([0-9]*)", True, ["E"], INPUT_DF4, None, ValueError, None],
])
def test_extract_scenarios(input_column, regular_expression, keep_original_value, output_columns, input_df, input_dtype, expected, expected_dtype, backend):
    input_df = backend.DataFrame(data=input_df, dtype=input_dtype)
    expected = backend.DataFrame(data=expected, dtype=expected_dtype) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, backend.impl.DataFrame):
            rule = backend.rules.StrExtractRule(
                input_column, regular_expression=regular_expression, keep_original_value=keep_original_value,
                output_columns=output_columns, named_input="input", named_output="result")
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule = backend.rules.StrExtractRule(
                    input_column, regular_expression=regular_expression, keep_original_value=keep_original_value,
                    output_columns=output_columns, named_input="input", named_output="result")
                rule.apply(data)
        else:
            assert False
