import dask.array as da
import dask.dataframe as dd
from pyarrow.lib import ArrowException

from etlrules.backends.common.newcolumns import (
    AddNewColumnRule as AddNewColumnRuleBase,
    AddRowNumbersRule as AddRowNumbersRuleBase,
)
from etlrules.backends.dask.expressions import Expression
from etlrules.backends.dask.types import MAP_TYPES


def _convert_partition(partition, dtype):
    try:
        return partition.astype(dtype)
    except (ValueError, TypeError, ArrowException) as exc:
        raise TypeError(str(exc)) from exc


class AddNewColumnRule(AddNewColumnRuleBase):

    def get_column_expression(self):
        return Expression(self.column_expression, filename=f'{self.output_column}_expression.py')

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_columns(df.columns)
        result = self._column_expression.eval(df)
        if self.column_type and not isinstance(result, dd.Series):
            # a constant: assign it to the column first, so that it can be converted to column_type
            result = df.assign(**{self.output_column: result})[self.output_column]
        if self.column_type:
            dtype = MAP_TYPES[self.column_type]
            try:
                # dask is lazy: a failed conversion is raised when the result is computed,
                # so the conversion of each partition maps the errors to TypeError
                result = result.map_partitions(_convert_partition, dtype, meta=result._meta.astype(dtype))
            except ValueError as exc:
                raise TypeError(str(exc)) from exc
        df = df.assign(**{self.output_column: result})
        self._set_output_df(data, df)


class AddRowNumbersRule(AddRowNumbersRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_columns(df.columns)
        stop = self.start + len(df.index) * self.step
        result = da.arange(self.start, stop, self.step)
        df = df.assign(**{self.output_column: result})
        self._set_output_df(data, df)
