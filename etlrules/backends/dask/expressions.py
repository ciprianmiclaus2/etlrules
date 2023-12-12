import dask.dataframe as dd
import logging
import pandas as pd

from etlrules.backends.common.expressions import Expression as ExpressionBase
from etlrules.data import context

perf_logger = logging.getLogger("etlrules.perf")


class Expression(ExpressionBase):

    def eval(self, df):
        try:
            expr_series = eval(self._compiled_expr, {}, {'df': df, 'context': context})
        except (TypeError, ValueError):
            # attempt to run a slower apply
            expr = self._compiled_expr
            if len(df.index) == 0:
                expr_series = dd.from_pandas(pd.Series([], dtype="string"), npartitions=1)
            else:
                perf_logger.warning("Evaluating expression '%s' is not vectorized and might hurt the overall performance.", self.expression_str)
                pandas_expr_series = df.head().apply(lambda df: eval(expr, {}, {'df': df, 'context': context}), axis=1)
                expr_series = df.apply(lambda df: eval(expr, {}, {'df': df, 'context': context}), axis=1, meta=("", pandas_expr_series.dtype))
        return expr_series
