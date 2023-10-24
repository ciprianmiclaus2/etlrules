import yaml
from typing import Optional

from .rule import BaseRule


class Plan:
    """ A plan to manipulate one or multiple dataframes with a set of rules.

    A plan is a blueprint on how to extract one or more dataframes from various sources (e.g. files or
    other data sources), how to transform those dataframes by adding calculated columns, joining
    different dataframe, aggregating, sorting, etc. and ultimately how to load that into a data store
    (files or other data stores).

    Args:
        name: A name for the plan. Optional.
        description: An optional documentation for the plan.
            This can include what the plan does, its purpose and detailed information about how it works.
        strict: A hint about how the plan should be executed.
            When None, then the plan has no hint to provide and its the caller deciding whether to run it
            in a strict mode or not.
    """

    def __init__(self, name: Optional[str]=None, description: Optional[str]=None, strict: Optional[bool]=None):
        self.name = name
        self.description = description
        self.strict = strict
        self.rules = []

    def add_rule(self, rule):
        assert isinstance(rule, BaseRule)
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
