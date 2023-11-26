import pytest

from etlrules.data import RuleData
from etlrules.engine import RuleEngine
from etlrules.exceptions import GraphRuntimeError, InvalidPlanError
from etlrules.plan import Plan

from tests.utils.data import assert_frame_equal


def test_run_simple_plan(backend):
    input_df = backend.DataFrame(data=[
        {'A': 2, 'B': 'n', 'C': True},
        {'A': 1, 'B': 'm', 'C': False},
        {'A': 3, 'B': 'p', 'C': True},
    ])
    data = RuleData(input_df)
    plan = Plan()
    plan.add_rule(backend.rules.SortRule(['A']))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B']))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}))
    rule_engine = RuleEngine(plan)
    valid, err = rule_engine.validate(data)
    assert valid is True
    assert err is None
    rule_engine.run(data)
    result = data.get_main_output()
    expected = backend.DataFrame(data=[
        {'AA': 1, 'BB': 'm'},
        {'AA': 2, 'BB': 'n'},
        {'AA': 3, 'BB': 'p'},
    ])
    assert_frame_equal(result, expected)


def test_run_simple_plan_named_inputs(backend):
    input_df = backend.DataFrame(data=[
        {'A': 2, 'B': 'n', 'C': True},
        {'A': 1, 'B': 'm', 'C': False},
        {'A': 3, 'B': 'p', 'C': True},
    ])
    data = RuleData(named_inputs={"input": input_df})
    plan = Plan()
    plan.add_rule(backend.rules.SortRule(['A'], named_input="input", named_output="sorted_data"))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="renamed_data"))
    rule_engine = RuleEngine(plan)
    valid, err = rule_engine.validate(data)
    assert valid is True
    assert err is None
    rule_engine.run(data)
    result = data.get_named_output("renamed_data")
    expected = backend.DataFrame(data=[
        {'AA': 1, 'BB': 'm'},
        {'AA': 2, 'BB': 'n'},
        {'AA': 3, 'BB': 'p'},
    ])
    assert_frame_equal(result, expected)


def test_mix_pipeline_graph_plan_types(backend):
    plan = Plan()
    plan.add_rule(backend.rules.SortRule(['A']))
    with pytest.raises(InvalidPlanError):
        plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))


def test_mix_graph_pipeline_plan_types(backend):
    plan = Plan()
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    with pytest.raises(InvalidPlanError):
        plan.add_rule(backend.rules.SortRule(['A']))


def test_run_empty_plan(backend):
    data = RuleData(named_inputs={"input": backend.DataFrame(data=[])})
    plan = Plan()
    rule_engine = RuleEngine(plan)
    with pytest.raises(InvalidPlanError) as exc:
        rule_engine.run(data)
    assert str(exc.value) == "An empty plan cannot be run."
    valid, err = rule_engine.validate(data)
    assert err is not None
    assert "An empty plan cannot be run." in err
    assert valid is False


def test_run_unknown_mode_plan(backend):
    data = RuleData(named_inputs={"input": backend.DataFrame(data=[])})
    plan = Plan()
    plan.add_rule(backend.rules.WriteCSVFileRule(file_name="test.csv.gz", file_dir="/home/myuser", separator=",", header=True, compression="gzip",
                named_input="result", name="BF", description="Some desc2 BF", strict=True))
    rule_engine = RuleEngine(plan)
    with pytest.raises(InvalidPlanError) as exc:
        rule_engine.run(data)
    assert str(exc.value) == "Plan's mode cannot be determined."
    valid, err = rule_engine.validate(data)
    assert err is not None
    assert "Plan's mode cannot be determined." in err
    assert valid is False


def test_run_simple_plan_named_inputs_different_order(backend):
    input_df = backend.DataFrame(data=[
        {'A': 2, 'B': 'n', 'C': True},
        {'A': 1, 'B': 'm', 'C': False},
        {'A': 3, 'B': 'p', 'C': True},
    ])
    data = RuleData(named_inputs={"input": input_df})
    plan = Plan()
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="renamed_data"))
    plan.add_rule(backend.rules.SortRule(['A'], named_input="input", named_output="sorted_data"))
    rule_engine = RuleEngine(plan)
    valid, err = rule_engine.validate(data)
    assert valid is True
    assert err is None
    rule_engine.run(data)
    result = data.get_named_output("renamed_data")
    expected = backend.DataFrame(data=[
        {'AA': 1, 'BB': 'm'},
        {'AA': 2, 'BB': 'n'},
        {'AA': 3, 'BB': 'p'},
    ])
    assert_frame_equal(result, expected)


def test_run_missing_named_input(backend):
    data = RuleData()
    plan = Plan()
    plan.add_rule(backend.rules.ReadCSVFileRule(file_name="test.csv.gz", file_dir="/home/myuser", named_output="input2"))
    plan.add_rule(backend.rules.WriteCSVFileRule(file_name="test.csv.gz", file_dir="/home/myuser", separator=",", header=True, compression="gzip",
                named_input="sorted_data", name="BF", description="Some desc2 BF", strict=True))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="renamed_data"))
    plan.add_rule(backend.rules.SortRule(['A'], named_input="input", named_output="sorted_data"))
    rule_engine = RuleEngine(plan)
    with pytest.raises(GraphRuntimeError) as exc:
        rule_engine.run(data)
    assert "requires a named_input=input which doesn't exist in the input data and it's not produced as a named output" in str(exc.value)
    valid, err = rule_engine.validate(data)
    assert err is not None
    assert "requires a named_input=input which doesn't exist in the input data and it's not produced as a named output" in err
    assert valid is False


def test_run_missing_named_input_in_rule(backend):
    data = RuleData()
    plan = Plan()
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}, named_output="renamed_data"))
    plan.add_rule(backend.rules.SortRule(['A'], named_input="input", named_output="sorted_data"))
    rule_engine = RuleEngine(plan)
    with pytest.raises(InvalidPlanError) as exc:
        rule_engine.run(data)
    assert "RenameRule" in str(exc.value)
    assert "has empty named input." in str(exc.value)
    valid, err = rule_engine.validate(data)
    assert err is not None
    assert "RenameRule" in err
    assert "has empty named input." in err
    assert valid is False


def test_run_missing_named_output_clashes(backend):
    data = RuleData()
    plan = Plan()
    plan.add_rule(backend.rules.SortRule(['A'], named_input="input", named_output="sorted_data"))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B', 'C'], named_input="input", named_output="projected_data"))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="renamed_data"))
    rule_engine = RuleEngine(plan)
    with pytest.raises(InvalidPlanError) as exc:
        rule_engine.run(data)
    assert "Named output 'projected_data' is produced by multiple rules" in str(exc.value)
    valid, err = rule_engine.validate(data)
    assert err is not None
    assert "Named output 'projected_data' is produced by multiple rules" in err
    assert valid is False

def test_run_produces_output_already_exists_in_input_data(backend):
    data = RuleData(named_inputs={"input": backend.DataFrame(data=[])})
    plan = Plan()
    plan.add_rule(backend.rules.SortRule(['A'], named_input="input", named_output="sorted_data"))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="input"))
    rule_engine = RuleEngine(plan)
    with pytest.raises(GraphRuntimeError) as exc:
        rule_engine.run(data)
    assert "Named output clashes. The following named outputs are produced by rules in the plan but they also exist in the input data, leading to ambiguity: {'input'}" in str(exc.value)
    valid, err = rule_engine.validate(data)
    assert err is not None
    assert "Named output clashes. The following named outputs are produced by rules in the plan but they also exist in the input data, leading to ambiguity: {'input'}" in err
    assert valid is False
