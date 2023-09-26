from .rule import BaseRule


class Plan:
    def __init__(self):
        self.rules = []

    def add_rule(self, rule):
        assert isinstance(rule, BaseRule)
        self.rules.append(rule)

    def __iter__(self):
        yield from self.rules

    def validate(self):
        errors = []
        return errors
