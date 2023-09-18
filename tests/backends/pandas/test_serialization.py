import pytest
from pandas import DataFrame

from finrules.backends.pandas import (
    DedupeRule, ProjectRule, RenameRule, SortRule, TypeConversionRule,
    RulesBlock,
)
from finrules.rule import BaseRule


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
    ]
)
def test_serialize(rule_instance):
    d = rule_instance.to_dict()
    instance = BaseRule.from_dict(d, backend='pandas')
    assert type(rule_instance) == type(instance)
    assert rule_instance == instance
    y = rule_instance.to_yaml()
    instance2 = BaseRule.from_yaml(y, backend='pandas')
    assert type(rule_instance) == type(instance2)
    assert rule_instance == instance2

