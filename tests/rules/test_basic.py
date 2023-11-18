import pytest
from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError, UnsupportedTypeError
from tests.utils.data import assert_frame_equal, get_test_data


@pytest.mark.parametrize("columns,exclude,main_input,named_inputs,named_input,named_output,expected", [
    [["A", "C", "E"], False, [{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}], None, None, None, [{"A": 1, "C": 3, "E": "e"}]],
    [["A", "C", "E"], False, [{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}], None, None, "result", [{"A": 1, "C": 3, "E": "e"}]],
    [["F", "C", "A", "D", "B", "E"], False, [{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}], None, None, None, [{"F": "f", "C": 3, "A": 1, "D": 4, "B": "b", "E": "e"}]],
    [["F", "C", "A", "D", "B", "E"], False, [{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}], None, None, "result", [{"F": "f", "C": 3, "A": 1, "D": 4, "B": "b", "E": "e"}]],
    [["A", "C", "E"], True, [{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}], None, None, None, [{"B": "b", "D": 4, "F": "f"}]],
    [["A", "C", "E"], True, [{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}], None, None, "result", [{"B": "b", "D": 4, "F": "f"}]],
    [["A", "C", "E"], False, [{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}], {"second_df": [{"A": 12, "B": "b2", "C": 32, "D": 42, "E": "e2", "F": "f2"}]}, "second_df", None, [{"A": 12, "C": 32, "E": "e2"}]],
    [["A", "C", "E"], False, {"A": [], "B": [], "C": [], "D": [], "E": [], "F": []}, None, None, None, {"A": [], "C": [], "E": []}],
])
def test_project_rule_scenarios(columns, exclude, main_input, named_inputs, named_input, named_output, expected, backend):
    main_input = backend.impl.DataFrame(data=main_input)
    named_inputs = {k: backend.impl.DataFrame(data=v) for k, v in named_inputs.items()} if named_inputs is not None else None
    expected = backend.impl.DataFrame(data=expected)
    with get_test_data(main_input, named_inputs=named_inputs, named_output=named_output) as data:
        rule = backend.rules.ProjectRule(columns, exclude=exclude, named_input=named_input, named_output=named_output)
        rule.apply(data)
        result = data.get_named_output(named_output) if named_output else data.get_main_output()
        assert_frame_equal(result, expected)


def test_project_rule_unknown_column_strict(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = backend.rules.ProjectRule(["A", "C", "UNKNOWN", "E"])
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_project_rule_unknown_column_not_strict(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = backend.rules.ProjectRule(["A", "C", "UNKNOWN", "E"], strict=False)
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"A": 1, "C": 3, "E": "e"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_unknown_column_exclude_strict(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = backend.rules.ProjectRule(["A", "C", "UNKNOWN", "E"], exclude=True)
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_project_rule_unknown_column_exclude_not_strict(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    assert list(df.columns) == ["A", "B", "C", "D", "E", "F"]
    with get_test_data(df) as data:
        rule = backend.rules.ProjectRule(["A", "C", "UNKNOWN", "E"], exclude=True, strict=False)
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"B": "b", "D": 4, "F": "f"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_project_rule_name_description(backend):
    rule = backend.rules.ProjectRule(["A", "C", "E"], name="Rule 1", description="This is the documentation for the rule")
    assert rule.get_name() == "Rule 1"
    assert rule.get_description() == "This is the documentation for the rule"


def test_rename_rule(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df) as data:
        rule = backend.rules.RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'})
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_rename_rule_empty_df(backend):
    df = backend.impl.DataFrame(data={"A": [], "B": [], "C": [], "D": [], "E": [], "F": []})
    with get_test_data(df) as data:
        rule = backend.rules.RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'})
        rule.apply(data)
        expected = backend.impl.DataFrame(data={"AA": [], "B": [], "CC": [], "D": [], "EE": [], "F": []})
        assert_frame_equal(data.get_main_output(), expected)


def test_rename_rule_named_input(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df, named_inputs={'other_data': df}) as data:
        rule = backend.rules.RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE'}, named_input='other_data', named_output="result")
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
        assert_frame_equal(data.get_named_output("result"), expected)


def test_rename_rule_strict_unknown_column(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df) as data:
        rule = backend.rules.RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'})
        with pytest.raises(MissingColumnError):
            rule.apply(data)


def test_rename_rule_non_strict_unknown_column(backend):
    df = backend.impl.DataFrame(data=[{"A": 1, "B": "b", "C": 3, "D": 4, "E": "e", "F": "f"}])
    with get_test_data(df) as data:
        rule = backend.rules.RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'}, strict=False)
        rule.apply(data)
        expected = backend.impl.DataFrame(data=[{"AA": 1, "B": "b", "CC": 3, "D": 4, "EE": "e", "F": "f"}])
        assert_frame_equal(data.get_main_output(), expected)


def test_rename_rule_name_description(backend):
    rule = backend.rules.RenameRule({'A': 'AA', 'C': 'CC', 'E': 'EE', 'UNKNOWN': 'NEW'}, name="Rule 1", description="This is the documentation for the rule")
    assert rule.get_name() == "Rule 1"
    assert rule.get_description() == "This is the documentation for the rule"


DEDUPE_KEEP_FIRST_INPUT_DF = [
    {"A": 1, "B": 1, "C": 1},
    {"A": 1, "B": 1, "C": 2},
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
]
DEDUPE_KEEP_FIRST_EXPECTED_DF = [
    {"A": 1, "B": 1, "C": 1},
    {"A": 2, "B": 3, "C": 4},
]
DEDUPE_KEEP_LAST_INPUT_DF = [
    {"A": 1, "B": 1, "C": 1},
    {"A": 1, "B": 1, "C": 2},
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
]
DEDUPE_KEEP_LAST_EXPECTED_DF = [
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
]
DEDUPE_KEEP_NONE_INPUT_DF = [
    {"A": 1, "B": 1, "C": 1},
    {"A": 1, "B": 1, "C": 2},
    {"A": 2, "B": 3, "C": 4},
    {"A": 1, "B": 1, "C": 3},
]
DEDUPE_KEEP_NONE_EXPECTED_DF = [
    {"A": 2, "B": 3, "C": 4},
]
DEDUPE_EMPTY_DF = {"A": [], "B": [], "C": []}


@pytest.mark.parametrize("columns,keep,input_df,named_input,named_output,expected", [
    [["A", "B"], "first", DEDUPE_KEEP_FIRST_INPUT_DF, None, None, DEDUPE_KEEP_FIRST_EXPECTED_DF],
    [["A", "B"], "first", DEDUPE_KEEP_FIRST_INPUT_DF, "input_df", None, DEDUPE_KEEP_FIRST_EXPECTED_DF],
    [["A", "B"], "first", DEDUPE_KEEP_FIRST_INPUT_DF, "input_df", "result", DEDUPE_KEEP_FIRST_EXPECTED_DF],
    [["A", "B"], "last", DEDUPE_KEEP_LAST_INPUT_DF, None, None, DEDUPE_KEEP_LAST_EXPECTED_DF],
    [["A", "B"], "last", DEDUPE_KEEP_LAST_INPUT_DF, "input_df", None, DEDUPE_KEEP_LAST_EXPECTED_DF],
    [["A", "B"], "last", DEDUPE_KEEP_LAST_INPUT_DF, "input_df", "result", DEDUPE_KEEP_LAST_EXPECTED_DF],
    [["A", "B"], "none", DEDUPE_KEEP_NONE_INPUT_DF, None, None, DEDUPE_KEEP_NONE_EXPECTED_DF],
    [["A", "B"], "none", DEDUPE_KEEP_NONE_INPUT_DF, "input_df", None, DEDUPE_KEEP_NONE_EXPECTED_DF],
    [["A", "B"], "none", DEDUPE_KEEP_NONE_INPUT_DF, "input_df", "result", DEDUPE_KEEP_NONE_EXPECTED_DF],
    [["A", "B"], "first", DEDUPE_EMPTY_DF, None, None, DEDUPE_EMPTY_DF],
])
def test_dedupe_rule_scenarios(columns, keep, input_df, named_input, named_output, expected, backend):
    input_df = backend.impl.DataFrame(input_df)
    with get_test_data(main_input=input_df, named_inputs=named_input and {named_input: input_df}, named_output=named_output) as data:
        rule = backend.rules.DedupeRule(columns, keep=keep, named_output=named_output)
        rule.apply(data)
        expected = backend.impl.DataFrame(expected)
        assert_frame_equal(data.get_main_output() if named_output is None else data.get_named_output(named_output), expected)


def test_dedupe_rule_raises_missing_column(backend):
    df = backend.impl.DataFrame(data=[
        {"A": 1, "B": 1, "C": 1},
        {"A": 1, "B": 1, "C": 2},
        {"A": 2, "B": 3, "C": 4},
        {"A": 1, "B": 1, "C": 3},
    ])
    with get_test_data(df) as data:
        rule = backend.rules.DedupeRule(["A", "B", "D"], keep='first', strict=False)
        with pytest.raises(MissingColumnError):
            rule.apply(data)


@pytest.mark.parametrize("input_column,values,new_values,regex,output_column,input_df,expected", [
    ["A", ["a", "b"], ["new_a", "bb"], False, None, 
        [{"A": "a", "B": 3}, {"A": "aa", "B": 1}],
        [{"A": "new_a", "B": 3}, {"A": "aa", "B": 1}]
    ],
    ["A", ["a", "aa"], ["new_a", "new_aa"], False, None, 
        [{"A": "a", "B": 3}, {"A": "aa", "B": 1}],
        [{"A": "new_a", "B": 3}, {"A": "new_aa", "B": 1}]
    ],
    ["B", [4, 1], [2, 2], False, "F", 
        [{"A": "a", "B": 3}, {"A": "aa", "B": 1}],
        [{"A": "a", "B": 3, "F": 3}, {"A": "aa", "B": 1, "F": 2}],
    ],
    ["A", ["a.*d"], ["new_a"], True, None, 
        [{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}],
        [{"A": "new_a", "B": 3}, {"A": "new_a", "B": 1}]
    ],
    ["A", [r"a(?P<n>.*)d"], [r"new_\g<n>_"], True, None, 
        [{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}],
        [{"A": "new_gag_", "B": 3}, {"A": "new_a_", "B": 1}]
    ],
    ["A", [r"a(?P<n>.*)d"], ["new_$n_"], True, None,
        [{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}],
        [{"A": "new_gag_", "B": 3}, {"A": "new_a_", "B": 1}]
    ],
    ["A", [r"a(?P<n>.*)d"], ["new_${n}_"], True, None, 
        [{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}],
        [{"A": "new_gag_", "B": 3}, {"A": "new_a_", "B": 1}]
    ],
    ["A", [r"a(.*)d"], [r"new_\1_"], True, None, 
        [{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}],
        [{"A": "new_gag_", "B": 3}, {"A": "new_a_", "B": 1}]
    ],
    ["A", [r"a(.*)d"], [r"new_$1_"], True, None, 
        [{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}],
        [{"A": "new_gag_", "B": 3}, {"A": "new_a_", "B": 1}]
    ],
    ["A", [r"a(.*)d"], [r"new_${1}_"], True, None, 
        [{"A": "agagd", "B": 3}, {"A": "aad", "B": 1}],
        [{"A": "new_gag_", "B": 3}, {"A": "new_a_", "B": 1}]
    ],
    ["Z", ["a", "b"], ["new_a", "bb"], False, None, 
        [{"A": "a", "B": 3}, {"A": "aa", "B": 1}],
        MissingColumnError
    ],
    ["A", ["a", "b"], ["new_a", "bb"], False, "A", 
        [{"A": "a", "B": 3}, {"A": "aa", "B": 1}],
        ColumnAlreadyExistsError
    ],
    ["A", [3, 1], [1, 2], False, None, 
        {"A": [], "B": []},
        {"A": [], "B": []},
    ],
])
def test_replace_scenarios(input_column, values, new_values, regex, output_column, input_df, expected, backend):
    input_df = backend.impl.DataFrame(data=input_df)
    expected = backend.impl.DataFrame(data=expected) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        rule = backend.rules.ReplaceRule(
            input_column=input_column, values=values, new_values=new_values, regex=regex,
            output_column=output_column, named_input="input", named_output="result")
        if isinstance(expected, backend.impl.DataFrame):
            rule.apply(data)
            assert_frame_equal(data.get_named_output("result"), expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule.apply(data)
        else:
            assert False


@pytest.mark.parametrize("input_column, column_type, input_df, input_types, expected, expected_types", [
    ["A", "int64", [
        {"A": [1, 2, 3]}, 
        {"A": [4, 5]},
        {"A": [6]}
    ], None, {"A": [1, 2, 3, 4, 5, 6]}, {"A": "Int64"}],
    ["A", "int64", [
        {"A": [1, 2, 3], "B": "str1"}, 
        {"A": [4, 5], "B": "str2"},
        {"A": [6], "B": "str3"}
    ], {"A": "list_int64s", "B": "string"}, {
        "A": [1, 2, 3, 4, 5, 6],
        "B": ["str1", "str1", "str1", "str2", "str2", "str3"],
    }, {"A": "Int64", "B": "string"}],
    ["B", "string", [
        {"A": 1, "B": ["str1", "str2", "str3"]}, 
        {"A": 2, "B": ["str4"]},
        {"A": 3, "B": ["str5"]}
    ], {"A": "Int64", "B": "list_strings"}, {
        "A": [1, 1, 1, 2, 3],
        "B": ["str1", "str2", "str3", "str4", "str5"],
    }, {"A": "Int64", "B": "string"}],

    # nullable tests
    ["A", "int64", [
        {"A": [1, 2, 3], "B": "str1"}, 
        {"B": "str2"},
        {"A": [6], "B": "str3"}
    ], {"A": "list_int64s", "B": "string"}, [
        {"A": 1, "B": "str1"},
        {"A": 2, "B": "str1"},
        {"A": 3, "B": "str1"},
        {"B": "str2"},
        {"A": 6, "B": "str3"}
    ], {"A": "Int64", "B": "string"}],

    # exceptions
    ["X", "int64", {"A": [1, 2, 3]}, {"A": "Int64"}, MissingColumnError, None],
    ["A", "unknown", {"A": [1, 2, 3]}, {"A": "Int64"}, UnsupportedTypeError, None],
])
def test_explode_rule(input_column, column_type, input_df, input_types, expected, expected_types, backend):
    input_df = backend.DataFrame(data=input_df, astype=input_types)
    expected = backend.DataFrame(data=expected, astype=expected_types) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, backend.impl.DataFrame):
            rule = backend.rules.ExplodeValuesRule(
                input_column=input_column, column_type=column_type,
                named_input="input", named_output="result")
            rule.apply(data)
            actual = data.get_named_output("result")
            assert_frame_equal(actual, expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected):
                rule = backend.rules.ExplodeValuesRule(
                    input_column=input_column, column_type=column_type,
                    named_input="input", named_output="result")
                rule.apply(data)
        else:
            assert False