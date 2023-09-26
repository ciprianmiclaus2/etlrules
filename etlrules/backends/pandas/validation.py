from pandas import DataFrame


class PandasRuleValidationMixin:
    def assert_is_dataframe(self, df, context):
        assert isinstance(df, DataFrame), context
