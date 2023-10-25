from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from etlrules.data import RuleData
from etlrules.engine import RuleEngine
from etlrules.exceptions import InvalidPlanError
from etlrules.plan import Plan
from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule


def test_run_simple_plan():
    input_df = DataFrame(data=[
        {'A': 2, 'B': 'n', 'C': True},
        {'A': 1, 'B': 'm', 'C': False},
        {'A': 3, 'B': 'p', 'C': True},
    ])
    data = RuleData(input_df)
    plan = Plan()
    plan.add_rule(SortRule(['A']))
    plan.add_rule(ProjectRule(['A', 'B']))
    plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}))
    rule_engine = RuleEngine(plan)
    rule_engine.run(data)
    result = data.get_main_output()
    expected = DataFrame(data=[
        {'AA': 1, 'BB': 'm'},
        {'AA': 2, 'BB': 'n'},
        {'AA': 3, 'BB': 'p'},
    ])
    assert_frame_equal(result, expected)


def test_run_simple_plan_named_inputs():
    input_df = DataFrame(data=[
        {'A': 2, 'B': 'n', 'C': True},
        {'A': 1, 'B': 'm', 'C': False},
        {'A': 3, 'B': 'p', 'C': True},
    ])
    data = RuleData(named_inputs={"input": input_df})
    plan = Plan()
    plan.add_rule(SortRule(['A'], named_input="input", named_output="sorted_data"))
    plan.add_rule(ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="renamed_data"))
    rule_engine = RuleEngine(plan)
    rule_engine.run(data)
    result = data.get_named_output("renamed_data")
    expected = DataFrame(data=[
        {'AA': 1, 'BB': 'm'},
        {'AA': 2, 'BB': 'n'},
        {'AA': 3, 'BB': 'p'},
    ])
    assert_frame_equal(result, expected)


def test_mix_pipeline_graph_plan_types():
    plan = Plan()
    plan.add_rule(SortRule(['A']))
    with pytest.raises(InvalidPlanError):
        plan.add_rule(ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))


def test_mix_graph_pipeline_plan_types():
    plan = Plan()
    plan.add_rule(ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    with pytest.raises(InvalidPlanError):
        plan.add_rule(SortRule(['A']))


def Xtest_run_simple_plan_named_inputs_different_order():
    input_df = DataFrame(data=[
        {'A': 2, 'B': 'n', 'C': True},
        {'A': 1, 'B': 'm', 'C': False},
        {'A': 3, 'B': 'p', 'C': True},
    ])
    data = RuleData(named_inputs={"input": input_df})
    plan = Plan()
    plan.add_rule(ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="renamed_data"))
    plan.add_rule(SortRule(['A'], named_input="input", named_output="sorted_data"))
    rule_engine = RuleEngine(plan)
    rule_engine.run(data)
    result = data.get_named_output("renamed_data")
    expected = DataFrame(data=[
        {'AA': 1, 'BB': 'm'},
        {'AA': 2, 'BB': 'n'},
        {'AA': 3, 'BB': 'p'},
    ])
    assert_frame_equal(result, expected)