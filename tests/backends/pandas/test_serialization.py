import pytest

from etlrules.backends.pandas import (
    DedupeRule, ProjectRule, RenameRule, ReplaceRule, SortRule, TypeConversionRule,
    RulesBlock, LeftJoinRule, InnerJoinRule, OuterJoinRule, RightJoinRule,
    ForwardFillRule, BackFillRule, AddNewColumnRule,
    VConcatRule, HConcatRule, AggregateRule, RoundRule, AbsRule,
    StrLowerRule, StrUpperRule, StrCapitalizeRule, StrStripRule, StrPadRule,
    StrSplitRule, StrSplitRejoinRule, StrExtractRule,
    IfThenElseRule, FilterRule,
    DateTimeLocalNowRule, DateTimeUTCNowRule, DateTimeToStrFormatRule,
    DateTimeRoundRule, DateTimeRoundDownRule, DateTimeRoundUpRule, 
    DateTimeAddRule, DateTimeSubstractRule, DateTimeDiffRule,
    DateTimeExtractComponentRule,
    ReadCSVFileRule, ReadParquetFileRule, WriteCSVFileRule, WriteParquetFileRule,
)
from etlrules.plan import Plan
from etlrules.rule import BaseRule


ALL_RULES = [
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
    RoundRule("A", 2, output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    AbsRule("B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrLowerRule("B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrUpperRule("B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrCapitalizeRule("B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrStripRule("B", how="both", characters="Ac", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrPadRule("B", width=8, fill_character=".", how="both", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrSplitRule("B", separator=";", limit=4, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrSplitRule("B", separator=",|;", limit=4, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrSplitRejoinRule("B", separator=";", limit=4, new_separator="|", sort="descending", output_column="G", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrSplitRejoinRule("B", separator=",|;", limit=4, new_separator="&", sort="ascending", output_column="G", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    ReplaceRule("B", values=["abc", 1], new_values=["aaa", 2], regex=False, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    ReplaceRule("B", values=["a.*d", "a.c"], new_values=[r"\1", r"a_\1_b"], regex=True, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    StrExtractRule("B", regular_expression="a(.*)d", keep_original_value=True, output_columns=["F"], named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    IfThenElseRule(condition_expression="df['A'] > df['B']", output_column="O", then_value="A is greater", else_value="B is greater", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    IfThenElseRule(condition_expression="df['A'] > df['B']", output_column="O", then_column="C", else_column="D", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    FilterRule(condition_expression="df['A'] > df['B']", discard_matching_rows=True, named_output_discarded="discarded", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeLocalNowRule(output_column="TimeNow", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeUTCNowRule(output_column="UTCTimeNow", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeToStrFormatRule(input_column="A", format="%Y-%m-%d %H:%M:%S", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeRoundRule(input_column="A", unit="day", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeRoundDownRule(input_column="A", unit="day", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeRoundUpRule(input_column="A", unit="day", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeAddRule(input_column="A", unit_value=40, unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeAddRule(input_column="A", unit_value="B", unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeSubstractRule(input_column="A", unit_value=40, unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeSubstractRule(input_column="A", unit_value="B", unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeDiffRule(input_column="A", input_column2="B", unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    DateTimeExtractComponentRule(input_column="A", component="day", locale="C", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    ReadCSVFileRule(file_name="test.csv", file_dir="/home/myuser", regex=False, separator=",", header=True, 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    ReadParquetFileRule(file_name="test.csv", file_dir="/home/myuser", regex=False, columns=["A", "B", "C"], filters=[["A", ">=", 10], ["B", "==", True]], 
                named_output="result", name="BF", description="Some desc2 BF", strict=True),
    WriteCSVFileRule(file_name="test.csv.gz", file_dir="/home/myuser", separator=",", header=True, compression="gzip",
                named_input="result", name="BF", description="Some desc2 BF", strict=True),
    WriteParquetFileRule(file_name="test.csv", file_dir="/home/myuser", compression="gzip", 
                named_input="result", name="BF", description="Some desc2 BF", strict=True),
]

@pytest.mark.parametrize(
    "rule_instance",
    ALL_RULES
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


def test_serialize_plan():
    plan = Plan(name="plan1", description="Some description.", strict=True)
    for rule in ALL_RULES:
        plan.add_rule(rule)
    dct = plan.to_dict()
    plan2 = Plan.from_dict(dct, "pandas")
    for rule1, rule2 in zip(plan.rules, plan2.rules):
        if rule1 != rule2:
            assert rule1.__dict__ == rule2.__dict__
    assert plan.__dict__ == plan2.__dict__
    assert plan == plan2
    yml = plan.to_yaml()
    plan3 = Plan.from_yaml(yml, "pandas")
    assert plan.__dict__ == plan3.__dict__
    assert plan == plan3
