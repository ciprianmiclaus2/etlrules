import polars as pl

from etlrules.backends.common.expressions import Expression as ExpressionBase


class Expression(ExpressionBase):

    def eval(self, df):
        try:
            expr_series = eval(self._compiled_expr, {}, {'df': df})
        except TypeError:
            # attempt to run a slower apply
            expr = self._compiled_expr
            if df.empty:
                expr_series = pl.Series([], dtype=pl.Utf8)
            else:
                expr_series = df.map_elements(lambda df: eval(expr, {}, {'df': df}))
        return expr_series
