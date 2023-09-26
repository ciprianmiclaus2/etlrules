from .data import RuleData


class RuleEngine:
    def __init__(self, plan):
        self.plan = plan

    def run(self, data):
        for rule in self.plan:
            rule.apply(data)
        return data
