import pyarrow as pa

from etlrules.backends.common.newcolumns import (
    AddNewColumnRule as AddNewColumnRuleBase,
    AddRowNumbersRule as AddRowNumbersRuleBase,
)
from etlrules.backends.pyarrow.base import set_column
from etlrules.backends.pyarrow.expressions import Expression, evaluate_to_column
from etlrules.backends.pyarrow.types import MAP_TYPES, cast


class AddNewColumnRule(AddNewColumnRuleBase):

    def get_column_expression(self):
        return Expression(self.column_expression, filename=f'{self.output_column}_expression.py')

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_columns(df.column_names)
        result = evaluate_to_column(self._column_expression, df)
        if self.column_type:
            try:
                result = cast(result, MAP_TYPES[self.column_type], strict=True)
            except ValueError as exc:
                raise TypeError(str(exc)) from exc
        df = set_column(df, self.output_column, result)
        self._set_output_df(data, df)


class AddRowNumbersRule(AddRowNumbersRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_columns(df.column_names)
        stop = self.start + df.num_rows * self.step
        result = pa.array(range(self.start, stop, self.step), type=pa.int64())
        df = set_column(df, self.output_column, result)
        self._set_output_df(data, df)
