import pytest

from tests.utils.data import assert_frame_equal, get_test_data


@pytest.mark.parametrize("named_input,named_output", [
    [None, None],
    ["input_df", None],
    [None, "result"],
    ["input_df", "result"]
])
def test_rules_block(named_input, named_output, backend):
    df = backend.DataFrame([
        {"A": "1", "B": "b", "C": 1},
        {"A": "2", "B": "b", "C": 2},
        {"A": "1", "B": "b", "C": 3},
        {"A": "3", "B": "b", "C": 4},
        {"A": "2", "B": "b", "C": 5},
    ])
    with get_test_data(df, named_inputs={"input_df": df}, named_output=named_output) as data:
        rule = backend.rules.RulesBlock(
            rules=[
                backend.rules.TypeConversionRule({"A": "int64"}),
                backend.rules.DedupeRule(["A", "B"]),
                backend.rules.ProjectRule(["A", "C"]),
                backend.rules.RenameRule({"A": "B"}),
                backend.rules.SortRule(["C"], ascending=False)
            ],
            named_input=named_input,
            named_output=named_output
        )
        rule.apply(data)
        expected = backend.DataFrame([
            {"B": 3, "C": 4},
            {"B": 2, "C": 2},
            {"B": 1, "C": 1},
        ], astype={"B": "Int64"})
        result = data.get_main_output() if named_output is None else data.get_named_output(named_output)
        assert_frame_equal(expected, result)
