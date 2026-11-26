import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.conditions import (
    IfThenElseRule as IfThenElseRuleBase,
    FilterRule as FilterRuleBase,
)
from etlrules.backends.pyarrow.base import set_column, to_column
from etlrules.backends.pyarrow.expressions import Expression, evaluate_to_column


def _condition(expression, df):
    cond = evaluate_to_column(expression, df)
    if not pa.types.is_boolean(cond.type):
        cond = pc.cast(cond, pa.bool_())
    # a null condition is not true
    return pc.fill_null(cond, False)


class IfThenElseRule(IfThenElseRuleBase):

    def get_condition_expression(self):
        return Expression(self.condition_expression, filename=f'{self.output_column}.py')

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_columns(df.column_names)
        cond = _condition(self._condition_expression, df)
        then_value = df[self.then_column] if self.then_value is None else self.then_value
        else_value = df[self.else_column] if self.else_value is None else self.else_value
        if not isinstance(then_value, pa.ChunkedArray) and not isinstance(else_value, pa.ChunkedArray):
            then_value = to_column(then_value, df.num_rows)
        result = pc.if_else(cond, then_value, else_value)
        df = set_column(df, self.output_column, result)
        self._set_output_df(data, df)


class FilterRule(FilterRuleBase):

    def get_condition_expression(self):
        return Expression(self.condition_expression, filename='FilterRule.py')

    def apply(self, data):
        df = self._get_input_df(data)
        cond = _condition(self._condition_expression, df)
        if self.discard_matching_rows:
            cond = pc.invert(cond)
        self._set_output_df(data, df.filter(cond))
        if self.named_output_discarded:
            data.set_named_output(self.named_output_discarded, df.filter(pc.invert(cond)))
