from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from finrules.data import RuleData
from finrules.backends.pandas import (
    DedupeRule, ProjectRule, RenameRule, RulesBlock, SortRule, TypeConversionRule
)


def test_rules_block():
    df = DataFrame(data=[
        {"A": "1", "B": "b", "C": 1},
        {"A": "2", "B": "b", "C": 2},
        {"A": "1", "B": "b", "C": 3},
        {"A": "3", "B": "b", "C": 4},
        {"A": "2", "B": "b", "C": 5},
    ])
    data = RuleData(df)
    rule = RulesBlock(
        rules=[
            TypeConversionRule({"A": "int64"}),
            DedupeRule(["A", "B"]),
            ProjectRule(["A", "C"]),
            RenameRule({"A": "B"}),
            SortRule(["C"], ascending=False)
        ]
    )
    rule.apply(data)
    expected = DataFrame(data=[
        {"B": 3, "C": 4},
        {"B": 2, "C": 2},
        {"B": 1, "C": 1},
    ])
    assert_frame_equal(expected, data.get_main_output())