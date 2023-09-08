from .data import RuleData


class BaseRule:

    def assert_is_dataframe(self, df, context):
        ...

    def apply(self, data):
        assert isinstance(data, RuleData)
