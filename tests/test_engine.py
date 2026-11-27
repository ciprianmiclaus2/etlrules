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


def test_run_graph_plan_with_joins_and_discarded_rows(backend):
    # a graph: the rules are added in a different order than they need to run, the join takes two named inputs
    # and the rows discarded by the filters are used by other rules (named_output_discarded)
    customers = backend.DataFrame([
        {"Id": 1, "Name": "a", "Country": "UK"},
        {"Id": 2, "Name": "b", "Country": "FR"},
        {"Id": 3, "Name": "c", "Country": "UK"},
    ], astype={"Id": "Int64", "Name": "string", "Country": "string"})
    orders = backend.DataFrame([
        {"Order": 10, "Id": 1, "Amount": 5},
        {"Order": 11, "Id": 2, "Amount": 50},
        {"Order": 12, "Id": 3, "Amount": 500},
    ], astype={"Order": "Int64", "Id": "Int64", "Amount": "Int64"})
    data = RuleData(named_inputs={"customers": customers, "orders": orders})
    plan = Plan()
    plan.add_rule(backend.rules.SortRule(["Order"], named_input="small_orders", named_output="small_orders_sorted"))
    plan.add_rule(backend.rules.SortRule(["Id"], named_input="non_uk_customers", named_output="non_uk_customers_sorted"))
    plan.add_rule(backend.rules.FilterRule("df['Amount'] > 10", named_input="uk_orders", named_output="big_orders",
                                           named_output_discarded="small_orders"))
    plan.add_rule(backend.rules.InnerJoinRule("orders", "uk_customers", key_columns_left=["Id"], named_output="uk_orders"))
    plan.add_rule(backend.rules.FilterValuesRule("Country", ["UK"], named_input="customers", named_output="uk_customers",
                                                 named_output_discarded="non_uk_customers"))
    engine = RuleEngine(plan)
    assert engine.validate(data) == (True, None)
    engine.run(data)
    assert_frame_equal(data.get_named_output("big_orders"), backend.DataFrame(
        [{"Order": 12, "Id": 3, "Amount": 500, "Name": "c", "Country": "UK"}],
        astype={"Order": "Int64", "Id": "Int64", "Amount": "Int64", "Name": "string", "Country": "string"}))
    assert_frame_equal(data.get_named_output("small_orders_sorted"), backend.DataFrame(
        [{"Order": 10, "Id": 1, "Amount": 5, "Name": "a", "Country": "UK"}],
        astype={"Order": "Int64", "Id": "Int64", "Amount": "Int64", "Name": "string", "Country": "string"}))
    assert_frame_equal(data.get_named_output("non_uk_customers_sorted"), backend.DataFrame(
        [{"Id": 2, "Name": "b", "Country": "FR"}], astype={"Id": "Int64", "Name": "string", "Country": "string"}))


def test_run_graph_plan_missing_join_input(backend):
    data = RuleData(named_inputs={"left": backend.DataFrame([{"A": 1}])})
    plan = Plan()
    plan.add_rule(backend.rules.LeftJoinRule("left", "right", key_columns_left=["A"], named_output="result"))
    valid, err = RuleEngine(plan).validate(data)
    assert valid is False
    assert "named_input=right" in err
    with pytest.raises(GraphRuntimeError):
        RuleEngine(plan).run(data)
