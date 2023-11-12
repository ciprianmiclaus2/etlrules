import polars as pl

from etlrules.backends.common.newcolumns import AddNewColumnRule as AddNewColumnRuleBase
from etlrules.backends.polars.expressions import Expression
from etlrules.backends.polars.types import MAP_TYPES


class AddNewColumnRule(AddNewColumnRuleBase):

    def get_column_expression(self):
        return Expression(self.column_expression, filename=f'{self.output_column}_expression.py')

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_columns(df.columns)
        try:
            result = self._column_expression.eval(df)
        except pl.exceptions.ColumnNotFoundError as exc:
            raise KeyError(str(exc))
        if self.column_type:
            try:
                result = result.cast(MAP_TYPES[self.column_type])
            except pl.exceptions.ComputeError as exc:
                raise TypeError(exc)
        df = df.with_columns(**{self.output_column: result})
        self._set_output_df(data, df)
