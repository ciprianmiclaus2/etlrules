import polars as pl

from etlrules.backends.common.fill import (
    BackFillRule as BackFillRuleBase,
    ForwardFillRule as ForwardFillRuleBase,
)


class FillMixin:
    def do_apply(self, df):
        df_columns = [col for col in df.columns]
        if self.sort_by:
            if isinstance(self.sort_ascending, bool):
                descending = not self.sort_ascending
            else:
                descending = [not asc for asc in self.sort_ascending]
            df = df.sort(by=self.sort_by, descending=descending)
        if self.group_by:
            columns = [col for col in df.columns if col not in self.group_by]
            df = df.group_by(self.group_by).agg(
                getattr(pl.col(col), self.FILL_METHOD)() if col in self.columns else pl.col(col) for col in columns
            ).explode(columns)
            df = df[df_columns]
        else:
            df = df.with_columns(*[
                getattr(pl.col(col), self.FILL_METHOD)() for col in self.columns
            ])
        return df


class ForwardFillRule(FillMixin, ForwardFillRuleBase):
    FILL_METHOD = "forward_fill"


class BackFillRule(FillMixin, BackFillRuleBase):
    FILL_METHOD = "backward_fill"
