from copy import deepcopy
import pytest

from etlrules.plan import Plan
from etlrules.rule import BaseRule


ALL_RULES = [
    ["DedupeRule", dict(columns=["A", "B"], named_input="Dedupe1", named_output="Dedupe2", name="Deduplicate", description="Some text", strict=True)],
    ["ProjectRule", dict(columns=["A", "B"], named_input="PR1", named_output="PR2", name="Project", description="Remove some cols", strict=False)],
    ["RenameRule", dict(mapper={"A": "B"}, named_input="RN1", named_output="RN2", name="Rename", description="Some desc", strict=True)],
    ["SortRule", dict(sort_by=["A", "B"], named_input="SR1", named_output="SR2", name="Sort", description="Some desc2", strict=True)],
    ["TypeConversionRule", dict(mapper={"A": "int64"}, named_input="TC1", named_output="TC2", name="Convert", description=None, strict=False)],
    ["RulesBlock", dict(
        rules=[
            ["DedupeRule", dict(columns=["A", "B"])],
            ["ProjectRule", dict(columns=["A", "B"])],
            ["RenameRule", dict(mapper={"A": "B"})],
            ["SortRule", dict(sort_by=["A", "B"])],
            ["TypeConversionRule", dict(mapper={"A": "int64"})],
        ],
        named_input="BC1", named_output="BC2", name="Block", description="Test", strict=False
    )],
    ["LeftJoinRule", dict(named_input_left="left1", named_input_right="right1",
                key_columns_left=["A", "C"], key_columns_right=["A", "B"], suffixes=["_x", "_y"],
                named_output="LJ2", name="LeftJoinRule", description="Some desc1", strict=True)],
    ["InnerJoinRule", dict(named_input_left="left2", named_input_right="right2",
                key_columns_left=["A", "D"], key_columns_right=["A", "B"], suffixes=["_x", None],
                named_output="IJ2", name="InnerJoinRule", description="Some desc2", strict=True)],
    ["OuterJoinRule", dict(named_input_left="left3", named_input_right="right3",
                key_columns_left=["A", "E"], key_columns_right=["A", "B"], suffixes=[None, "_y"],
                named_output="OJ2", name="OuterJoinRule", description="Some desc3", strict=True)],
    ["RightJoinRule", dict(named_input_left="left4", named_input_right="right4",
                key_columns_left=["A", "F"], suffixes=["_x", "_y"],
                named_output="RJ2", name="RightJoinRule", description="Some desc4", strict=True)],
    ["ForwardFillRule", dict(columns=["A", "B"], sort_by=["C", "D"], sort_ascending=False, group_by=["Z", "X"],
                    named_input="FF1", named_output="FF2", name="FF", description="Some desc2 FF", strict=True)],
    ["BackFillRule", dict(columns=["A", "C"], sort_by=["E", "F"], sort_ascending=True, group_by=["Y", "X"], 
                    named_input="BF1", named_output="BF2", name="BF", description="Some desc2 BF", strict=True)],
    ["AddNewColumnRule", dict(output_column="NEW_COL", column_expression="df['A'] + df['B']", column_type="int64",
                        named_input="BF1", named_output="BF2", name="BF", description="Some desc2 BF", strict=True)],
    ["VConcatRule", dict(named_input_left="left4", named_input_right="right4", subset_columns=["A", "F"],
                 named_output="RJ2", name="RightJoinRule", description="Some desc4", strict=True)],
    ["HConcatRule", dict(named_input_left="left4", named_input_right="right4",
                named_output="RJ2", name="RightJoinRule", description="Some desc4", strict=True)],
    ["AggregateRule", dict(
        group_by=["A", "Col B"],
        aggregations={"D": "sum", "E": "last", "F": "csv"},
        aggregation_expressions={
            "C2": "sum(v**2 for v in values)",
            "D2": "';'.join(values)",
            "E2": "int(sum(v**2 for v in values if not isnull(v)))",
            "F3": "':'.join(v for v in values if not isnull(v))"
        },
        aggregation_types={"E": "string", "E2": "int64"},
        named_input="BF1", named_output="BF2", name="BF", description="Some desc2 BF", strict=True)],
    ["RoundRule", dict(input_column="A", scale=2, output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["AbsRule", dict(input_column="B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrLowerRule", dict(input_column="B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrUpperRule", dict(input_column="B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrCapitalizeRule", dict(input_column="B", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrStripRule", dict(input_column="B", how="both", characters="Ac", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrPadRule", dict(input_column="B", width=12, fill_character=".", how="left", output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrSplitRule", dict(input_column="B", separator=";", limit=4, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrSplitRule", dict(input_column="B", separator=",|;", limit=4, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrSplitRejoinRule", dict(input_column="B", separator=";", limit=4, new_separator="|", sort="descending", output_column="G", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrSplitRejoinRule", dict(input_column="B", separator=",|;", limit=4, new_separator="&", sort="ascending", output_column="G", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["ReplaceRule", dict(input_column="B", values=["abc", 1], new_values=["aaa", 2], regex=False, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["ReplaceRule", dict(input_column="B", values=["a.*d", "a.c"], new_values=[r"\1", r"a_\1_b"], regex=True, output_column="F", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["StrExtractRule", dict(input_column="B", regular_expression="a(.*)d", keep_original_value=True, output_columns=["F"], named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["IfThenElseRule", dict(condition_expression="df['A'] > df['B']", output_column="O", then_value="A is greater", else_value="B is greater", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["IfThenElseRule", dict(condition_expression="df['A'] > df['B']", output_column="O", then_column="C", else_column="D", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["FilterRule", dict(condition_expression="df['A'] > df['B']", discard_matching_rows=True, named_output_discarded="discarded", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeLocalNowRule", dict(output_column="TimeNow", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeUTCNowRule", dict(output_column="UTCTimeNow", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeToStrFormatRule", dict(input_column="A", format="%Y-%m-%d %H:%M:%S", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeRoundRule", dict(input_column="A", unit="day", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeRoundDownRule", dict(input_column="A", unit="day", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeRoundUpRule", dict(input_column="A", unit="day", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeAddRule", dict(input_column="A", unit_value=40, unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeAddRule", dict(input_column="A", unit_value="B", unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeSubstractRule", dict(input_column="A", unit_value=40, unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeSubstractRule", dict(input_column="A", unit_value="B", unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeDiffRule", dict(input_column="A", input_column2="B", unit="days", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["DateTimeExtractComponentRule", dict(input_column="A", component="day", locale="en_US.utf8", output_column="E", named_input="input", 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["ReadCSVFileRule", dict(file_name="test.csv", file_dir="/home/myuser", regex=False, separator=",", header=True, 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["ReadParquetFileRule", dict(file_name="test.csv", file_dir="/home/myuser", regex=False, columns=["A", "B", "C"], filters=[["A", ">=", 10], ["B", "==", True]], 
                named_output="result", name="BF", description="Some desc2 BF", strict=True)],
    ["WriteCSVFileRule", dict(file_name="test.csv.gz", file_dir="/home/myuser", separator=",", header=True, compression="gzip",
                named_input="result", name="BF", description="Some desc2 BF", strict=True)],
    ["WriteParquetFileRule", dict(file_name="test.csv", file_dir="/home/myuser", compression="gzip", 
                named_input="result", name="BF", description="Some desc2 BF", strict=True)],
    ["ReadSQLQueryRule", dict(sql_engine="sqlite:///mydb.db", sql_query="SELECT * FROM MyTable", named_output="MyData", name="BF", description="Some desc2 BF", strict=True)],
    ["WriteSQLTableRule", dict(sql_engine="sqlite:///mydb.db", sql_table="MyTable", if_exists="append", named_input="input_data", name="BF", description="Some desc2 BF", strict=True)],
    ["ExplodeValuesRule", dict(input_column="to_explode", column_type="int64", named_input="input", named_output="result", name="name", description="description", strict=True)],
    ["AddRowNumbersRule", dict(output_column="row_number", start=10, step=1, named_input="input", named_output="result", name="name", description="description", strict=True)],
]


def get_rule_instance(rule_cls_str, rule_args_dict, bk):
    rule_cls = getattr(bk.rules, rule_cls_str)
    if rule_cls_str == "RulesBlock":
        rule_args_dict = deepcopy(rule_args_dict)
        rules = rule_args_dict.pop("rules")
        rule_args_dict["rules"] = [get_rule_instance(rule_name, rule_args, bk) for rule_name, rule_args in rules]
    rule_instance = rule_cls(**rule_args_dict)
    return rule_instance


@pytest.mark.parametrize(
    "rule_cls_str, rule_args_dict",
    ALL_RULES
)
def test_serialize(rule_cls_str, rule_args_dict, backend):
    rule_instance = get_rule_instance(rule_cls_str, rule_args_dict, backend)
    d = rule_instance.to_dict()
    instance = BaseRule.from_dict(d, backend=backend.name)
    assert type(rule_instance) == type(instance)
    assert rule_instance == instance, "%s != %s" % (rule_instance.__dict__, instance.__dict__)
    y = rule_instance.to_yaml()
    instance2 = BaseRule.from_yaml(y, backend=backend.name)
    assert type(rule_instance) == type(instance2)
    assert rule_instance == instance2, "%s != %s" % (rule_instance.__dict__, instance2.__dict__)


def test_serialize_plan(backend):
    plan = Plan(name="plan1", description="Some description.", context={"str_val": "val1", "int_val": 1, "float_val": 2.5, "bool_val": True}, strict=True)
    for rule_cls_str, rule_args_dict in ALL_RULES:
        plan.add_rule(get_rule_instance(rule_cls_str, rule_args_dict, backend))
    dct = plan.to_dict()
    plan2 = Plan.from_dict(dct, backend.name)
    for rule1, rule2 in zip(plan.rules, plan2.rules):
        if rule1 != rule2:
            assert rule1.__dict__ == rule2.__dict__
    assert plan.__dict__ == plan2.__dict__
    assert plan == plan2
    yml = plan.to_yaml()
    plan3 = Plan.from_yaml(yml, backend.name)
    assert plan.__dict__ == plan3.__dict__
    assert plan == plan3
