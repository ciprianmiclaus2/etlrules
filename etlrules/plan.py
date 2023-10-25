import yaml
from typing import Literal, Optional

from .exceptions import InvalidPlanError
from .rule import BaseRule


class PlanMode:
    PIPELINE = "pipeline"
    GRAPH = "graph"


def plan_type_from_rule(rule: BaseRule) -> Optional[Literal[PlanMode.PIPELINE, PlanMode.GRAPH]]:
    if rule and rule.has_output():
        return PlanMode.GRAPH if rule.has_named_output() else PlanMode.PIPELINE


class Plan:
    """ A plan to manipulate one or multiple dataframes with a set of rules.

    A plan is a blueprint on how to extract one or more dataframes from various sources (e.g. files or
    other data sources), how to transform those dataframes by adding calculated columns, joining
    different dataframe, aggregating, sorting, etc. and ultimately how to load that into a data store
    (files or other data stores).

    A plan can operate in two modes: pipeline or graph. A pipeline graph is a simple type of plan where
    each rule take its input from the previous rule's output. A graph plan is more complex as it allows
    rules to produce named outputs which can then be used by other rules. This ultimately builds a dag
    (directed acyclic graph) of rule dependencies. A graph allows branching and joining back allowing
    complex logic. Rules are executed in the order of dependency and not in the order they are added to
    the plan. By comparison, pipelines implement a single input/single output mode where rules are
    executed in the order they are added to the plan.

    Pipeline example::

        plan = Plan()
        plan.add_rule(SortRule(['A']))
        plan.add_rule(ProjectRule(['A', 'B']))
        plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}))
    
    Graph example::

        plan = Plan()
        plan.add_rule(SortRule(['A'], named_input="input", named_output="sorted_data"))
        plan.add_rule(ProjectRule(['A', 'B'], named_input="sorted_data", named_output="projected_data"))
        plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}, named_input="projected_data", named_output="renamed_data"))

    Note:
        Rules that are used in graph mode should take a named_input and produce a named_output. Rules
        that use the pipeline mode must not used named inputs/outputs. The two type of rules cannot be
        used in the same plan as that leads to ambiguity.

    Args:
        name: A name for the plan. Optional.
        description: An optional documentation for the plan.
            This can include what the plan does, its purpose and detailed information about how it works.
        strict: A hint about how the plan should be executed.
            When None, then the plan has no hint to provide and its the caller deciding whether to run it
            in a strict mode or not.

    Raises:
        InvalidPlanError: if pipeline mode rules are mixed with graph mode rules
    """

    def __init__(self, name: Optional[str]=None, description: Optional[str]=None, strict: Optional[bool]=None):
        self.name = name
        self.description = description
        self.strict = strict
        self.rules = []

    def _check_plan_type(self, rule):
        _plan_type = None
        for r in self.rules:
            _plan_type = plan_type_from_rule(r)
            if _plan_type is not None:
                break
        if _plan_type is not None:
            _new_plan_type = plan_type_from_rule(rule)
            if _new_plan_type is not None and _plan_type != _new_plan_type:
                raise InvalidPlanError(f"Mixing of rules taking named inputs and rules with no named inputs is not supported. ({self.rules[0].__class__} vs. {rule.__class__})")

    def add_rule(self, rule):
        assert isinstance(rule, BaseRule)
        self._check_plan_type(rule)
        self.rules.append(rule)

    def __iter__(self):
        yield from self.rules

    def to_dict(self):
        rules = [rule.to_dict() for rule in self.rules]
        return {
            "name": self.name,
            "description": self.description,
            "strict": self.strict,
            "rules": rules
        }

    @classmethod
    def from_dict(cls, dct, backend):
        instance = Plan(
            name=dct.get("name"),
            description=dct.get("description"),
            strict=dct.get("strict")
        )
        rules = dct.get("rules", ())
        for rule in rules:
            instance.add_rule(BaseRule.from_dict(rule, backend))
        return instance

    def to_yaml(self):
        return yaml.safe_dump(self.to_dict())

    @classmethod
    def from_yaml(cls, yml, backend):
        dct = yaml.safe_load(yml)
        return cls.from_dict(dct, backend)

    def __eq__(self, other):
        return (
            type(self) == type(other) and 
            self.name == other.name and self.description == other.description and
            self.strict == other.strict and self.rules == other.rules
        )

    def __hash__(self):
        return hash((self.name, self.description, self.strict, self.rules))
