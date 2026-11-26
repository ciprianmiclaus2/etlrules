import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.nulls import (
    CoalesceRule as CoalesceRuleBase,
    DropNullRowsRule as DropNullRowsRuleBase,
    FillNullRule as FillNullRuleBase,
)
from etlrules.backends.pyarrow.base import set_column

TYPE_ERRORS = (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError)


class FillNullRule(FillNullRuleBase):

    def do_fill(self, df):
        for col in self.columns:
            column = df[col]
            if pa.types.is_null(column.type):
                # a column with only nulls has no type: the value gives it one
                filled = pa.array([self.value] * len(column))
            else:
                try:
                    filled = pc.fill_null(column, pa.scalar(self.value, type=column.type))
                except TYPE_ERRORS as exc:
                    raise TypeError(f"Cannot fill the nulls in column '{col}' of type {column.type} with {self.value!r}: {exc}") from exc
            df = set_column(df, col, filled)
        return df


class DropNullRowsRule(DropNullRowsRuleBase):

    def do_drop(self, df, columns):
        combine = pc.and_ if self.how == self.HOW_ANY else pc.or_
        keep = None
        for col in columns:
            valid = pc.is_valid(df[col])
            keep = valid if keep is None else combine(keep, valid)
        if keep is None:
            return df
        return df.filter(keep)


class CoalesceRule(CoalesceRuleBase):

    def do_coalesce(self, df):
        columns = [df[col] for col in self.columns]
        # columns with only nulls (null type) take the type of the other columns
        types = [column.type for column in columns if not pa.types.is_null(column.type)]
        if types:
            columns = [pc.cast(column, types[0]) if pa.types.is_null(column.type) else column for column in columns]
        try:
            result = pc.coalesce(*columns)
        except TYPE_ERRORS as exc:
            raise TypeError(f"Cannot coalesce columns {self.columns} of different types: {exc}") from exc
        return set_column(df, self.output_column, result)
