import pytest

from etlrules.exceptions import InvalidPlanError
from etlrules.plan import Plan


def test_plan_rules(backend):
    plan = Plan(name="Test Plan", description="This is a plan to test the plan.", strict=True)
    rules_in = [
        backend.rules.SortRule(['A']),
        backend.rules.ProjectRule(['A', 'B']),
        backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}),
    ]
    for rule in rules_in:
        plan.add_rule(rule)
    for idx, rule in enumerate(plan):
        assert rule == rules_in[idx]


def test_plan_to_from_dict(backend):
    plan = Plan(name="Test Plan", description="This is a plan to test the plan.", strict=True)
    plan.add_rule(backend.rules.SortRule(['A']))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B']))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}))
    dct = plan.to_dict()
    assert dct["name"] == "Test Plan"
    assert dct["description"] == "This is a plan to test the plan."
    assert dct["strict"] == True
    assert len(dct["rules"]) == 3
    plan2 = Plan.from_dict(dct, backend.name)
    assert plan == plan2


def test_plan_to_from_yaml(backend):
    plan = Plan(name="Test Plan", description="This is a plan to test the plan.", strict=True)
    plan.add_rule(backend.rules.SortRule(['A']))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B']))
    plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}))
    yml = plan.to_yaml()
    plan2 = Plan.from_yaml(yml, backend.name)
    assert plan == plan2


def test_graph_plan_rule_without_named_output(backend):
    plan = Plan()
    plan.add_rule(backend.rules.SortRule(['A'], named_input="input", named_output="sorted_data"))
    plan.add_rule(backend.rules.ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
    with pytest.raises(InvalidPlanError) as exc:
        plan.add_rule(backend.rules.RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data"))
    assert "Mixing of rules taking named inputs and rules with no named inputs is not supported." in str(exc.value)
