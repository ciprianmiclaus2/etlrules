import pytest

from etlrules.backends.pandas import (
    DedupeRule, ProjectRule, RenameRule, SortRule, TypeConversionRule,
    RulesBlock, LeftJoinRule, InnerJoinRule, OuterJoinRule, RightJoinRule,
    ForwardFillRule, BackFillRule, AddNewColumnRule,
    VConcatRule, HConcatRule, AggregateRule, RoundRule, AbsRule,
    StrLowerRule, StrUpperRule, StrCapitalizeRule,
)
from etlrules.rule import BaseRule


@pytest.mark.parametrize(
    "rule_instance",
    [
        DedupeRule(["A", "B"], named_input="Dedupe1", named_output="Dedupe2", name="Deduplicate", description="Some text", strict=True),
        ProjectRule(["A", "B"], named_input="PR1", named_output="PR2", name="Project", description="Remove some cols", strict=False),
        RenameRule({"A": "B"}, named_input="RN1", named_output="RN2", name="Rename", description="Some desc", strict=True),
        SortRule(["A", "B"], named_input="SR1", named_output="SR2", name="Sort", description="Some desc2", strict=True),
        TypeConversionRule({"A": "int64"}, named_input="TC1", named_output="TC2", name="Convert", description=None, strict=False),
        RulesBlock(
            rules=[DedupeRule(["A", "B"]), ProjectRule(["A", "B"]), RenameRule({"A": "B"}), SortRule(["A", "B"]), TypeConversionRule({"A": "int64"})],
            named_input="BC1", named_output="BC2", name="Block", description="Test", strict=False
        ),
        LeftJoinRule(named_input_left="left1", named_input_right="right1",
                    key_columns_left=["A", "C"], key_columns_right=["A", "B"], suffixes=["_x", "_y"],
                    named_output="LJ2", name="LeftJoinRule", description="Some desc1", strict=True),
        InnerJoinRule(named_input_left="left2", named_input_right="right2",
                    key_columns_left=["A", "D"], key_columns_right=["A", "B"], suffixes=["_x", None],
                    named_output="IJ2", name="InnerJoinRule", description="Some desc2", strict=True),
        OuterJoinRule(named_input_left="left3", named_input_right="right3",
                    key_columns_left=["A", "E"], key_columns_right=["A", "B"], suffixes=[None, "_y"],
                    named_output="OJ2", name="OuterJoinRule", description="Some desc3", strict=True),
        RightJoinRule(named_input_left="left4", named_input_right="right4",
                    key_columns_left=["A", "F"], suffixes=["_x", "_y"],
                    named_output="RJ2", name="RightJoinRule", description="Some desc4", strict=True),
        ForwardFillRule(["A", "B"], sort_by=["C", "D"], sort_ascending=False, group_by=["Z", "X"],
                        named_input="FF1", named_output="FF2", name="FF", description="Some desc2 FF", strict=True),
        BackFillRule(["A", "C"], sort_by=["E", "F"], sort_ascending=True, group_by=["Y", "X"], 
                     named_input="BF1", named_output="BF2", name="BF", description="Some desc2 BF", strict=True),
        AddNewColumnRule("NEW_COL", "df['A'] + df['B']",
                         named_input="BF1", named_output="BF2", name="BF", description="Some desc2 BF", strict=True),
        VConcatRule(named_input_left="left4", named_input_right="right4", subset_columns=["A", "F"],
                    named_output="RJ2", name="RightJoinRule", description="Some desc4", strict=True),
        HConcatRule(named_input_left="left4", named_input_right="right4",
                    named_output="RJ2", name="RightJoinRule", description="Some desc4", strict=True),
        AggregateRule(
            group_by=["A", "Col B"],
            aggregations={"D": "sum", "E": "last", "F": "csv"},
            aggregation_expressions={
                "C2": "sum(v**2 for v in values)",
                "D2": "';'.join(values)",
                "E2": "int(sum(v**2 for v in values if not isnull(v)))",
                "F3": "':'.join(v for v in values if not isnull(v))"
            },
            named_input="BF1", named_output="BF2", name="BF", description="Some desc2 BF", strict=True),
        RoundRule({"A": 2, "B": 0, "C": 3}, named_input="input", 
                  named_output="result", name="BF", description="Some desc2 BF", strict=True),
        AbsRule(["B", "D", "Z"], named_input="input", 
                  named_output="result", name="BF", description="Some desc2 BF", strict=True),
        StrLowerRule(["B", "D", "Z"], named_input="input", 
                  named_output="result", name="BF", description="Some desc2 BF", strict=True),
        StrUpperRule(["B", "D", "Z"], named_input="input", 
                  named_output="result", name="BF", description="Some desc2 BF", strict=True),
        StrCapitalizeRule(["B", "D", "Z"], named_input="input", 
                  named_output="result", name="BF", description="Some desc2 BF", strict=True),
    ]
)
def test_serialize(rule_instance):
    d = rule_instance.to_dict()
    instance = BaseRule.from_dict(d, backend='pandas')
    assert type(rule_instance) == type(instance)
    assert rule_instance == instance, "%s != %s" % (rule_instance.__dict__, instance.__dict__)
    y = rule_instance.to_yaml()
    instance2 = BaseRule.from_yaml(y, backend='pandas')
    assert type(rule_instance) == type(instance2)
    assert rule_instance == instance2, "%s != %s" % (rule_instance.__dict__, instance2.__dict__)

