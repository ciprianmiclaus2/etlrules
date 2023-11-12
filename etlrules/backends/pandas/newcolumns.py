from etlrules.backends.common.newcolumns import AddNewColumnRule as AddNewColumnRuleBase
from etlrules.backends.pandas.expressions import Expression
from etlrules.backends.pandas.types import MAP_TYPES

class AddNewColumnRule(AddNewColumnRuleBase):

    def get_column_expression(self):
        return Expression(self.column_expression, filename=f'{self.output_column}_expression.py')

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_columns(df.columns)
        result = self._column_expression.eval(df)
        if self.column_type:
            try:
                result = result.astype(MAP_TYPES[self.column_type])
            except ValueError as exc:
                raise TypeError(str(exc))
        df = df.assign(**{self.output_column: result})
        self._set_output_df(data, df)
