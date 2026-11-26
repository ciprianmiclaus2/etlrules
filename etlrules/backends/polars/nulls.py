import polars as pl

from etlrules.backends.common.nulls import (
    CoalesceRule as CoalesceRuleBase,
    DropNullRowsRule as DropNullRowsRuleBase,
    FillNullRule as FillNullRuleBase,
)


class FillNullRule(FillNullRuleBase):

    def do_fill(self, df):
        return df.with_columns(pl.col(col).fill_null(self.value) for col in self.columns)


class DropNullRowsRule(DropNullRowsRuleBase):

    def do_drop(self, df, columns):
        if self.how == self.HOW_ANY:
            return df.drop_nulls(subset=columns)
        return df.filter(~pl.all_horizontal(pl.col(col).is_null() for col in columns))


class CoalesceRule(CoalesceRuleBase):

    def do_coalesce(self, df):
        return df.with_columns(pl.coalesce(self.columns).alias(self.output_column))
