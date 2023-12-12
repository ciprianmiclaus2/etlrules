import logging
import polars as pl

from etlrules.backends.common.expressions import Expression as ExpressionBase
from etlrules.data import context

perf_logger = logging.getLogger("etlrules.perf")


class Expression(ExpressionBase):

    def eval(self, df):
        try:
            expr_series = eval(self._compiled_expr, {}, {'df': df, 'context': context})
        except (TypeError, pl.exceptions.SchemaError):
            # attempt to run a slower apply
            expr = self._compiled_expr
            if df.is_empty():
                expr_series = pl.Series([], dtype=pl.Utf8)
            else:
                columns = list(df.columns)
                perf_logger.warning("Evaluating expression '%s' is not vectorized and might hurt the overall performance.", self.expression_str)
                df_out = df.map_rows(lambda df: eval(expr, {}, {'df': dict(zip(columns, df)), 'context': context}))
                expr_series = df_out[df_out.columns[0]]
        return expr_series
