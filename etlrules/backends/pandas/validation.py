from pandas import DataFrame


class PandasRuleValidationMixin:
    def assert_is_dataframe(self, df: DataFrame, context: str):
        assert isinstance(df, DataFrame), context
