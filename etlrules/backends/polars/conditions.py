import polars as pl

from etlrules.backends.common.conditions import (
    IfThenElseRule as IfThenElseRuleBase,
    FilterRule as FilterRuleBase
)

from etlrules.backends.polars.expressions import Expression


class IfThenElseRule(IfThenElseRuleBase):

    def get_condition_expression(self):
        return Expression(self.condition_expression, filename=f'{self.output_column}.py')

    def apply(self, data):
        df = self._get_input_df(data)
        df_columns = set(df.columns)
        self._validate_columns(df_columns)
        try:
            cond_series = self._condition_expression.eval(df)
        except pl.exceptions.ColumnNotFoundError as exc:
            raise KeyError(str(exc))
        then_value = pl.lit(self.then_value) if self.then_value is not None else pl.col(self.then_column)
        else_value = pl.lit(self.else_value) if self.else_value is not None else pl.col(self.else_column)
        result = pl.when(cond_series).then(then_value).otherwise(else_value)
        df = df.with_columns(**{self.output_column: result})
        self._set_output_df(data, df)


class FilterRule(FilterRuleBase):

    def get_condition_expression(self):
        return Expression(self.condition_expression, filename="FilterRule.py")

    def apply(self, data):
        df = self._get_input_df(data)
        try:
            cond_series = self._condition_expression.eval(df)
        except pl.exceptions.ColumnNotFoundError as exc:
            raise KeyError(str(exc))
        if self.discard_matching_rows:
            cond_series = ~cond_series
        result = df.filter(cond_series)
        self._set_output_df(data, result)
        if self.named_output_discarded:
            discarded_result = df.filter(~cond_series)
            data.set_named_output(self.named_output_discarded, discarded_result)
