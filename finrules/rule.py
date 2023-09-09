from .data import RuleData


class BaseRule:
    def __init__(self, strict=True):
        self.strict = strict

    def assert_is_dataframe(self, df, context):
        ...

    def apply(self, data):
        assert isinstance(data, RuleData)
