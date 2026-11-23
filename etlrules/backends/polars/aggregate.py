import polars as pl

from etlrules.backends.common.aggregate import AggregateRule as AggregateRuleBase
from etlrules.backends.polars.types import MAP_TYPES


def listify(col):
    # in a group_by context, a column expression which is not aggregated produces a list per group
    return pl.col(col).drop_nulls()

def stringify(col):
    return pl.col(col).drop_nulls().cast(pl.Utf8).str.join(",")


class AggregateRule(AggregateRuleBase):

    AGGREGATIONS = {
        "min": "min",
        "max": "max",
        "mean": "mean",
        "count": [("fill_null", {"strategy": "zero"}), "count"],
        "countNoNA": ["is_not_null", "sum"],
        "sum": "sum",
        "first": "first",
        "last": "last",
        "list": listify,
        "csv": stringify,
    }

    def _get_agg(self, col, aggs, kwargs):
        if isinstance(aggs, str):
            return getattr(pl.col(col), aggs)(**kwargs)
        elif isinstance(aggs, list):
            expr = None
            for agg in aggs:
                kwargs = {}
                if isinstance(agg, tuple):
                    agg, kwargs = agg
                if expr is None:
                    expr = self._get_agg(col, agg, kwargs)
                else:
                    expr = getattr(expr, agg)(**kwargs)
            return expr
        elif aggs in (listify, stringify):
            return aggs(col)
        # aggregation expression: collect the values of each group as a list
        # and evaluate the expression afterwards (see _apply_aggregation_expressions)
        return pl.col(col)

    def _apply_aggregation_expressions(self, result, aggs):
        expression_columns = [
            col for col, agg in aggs.items()
            if callable(agg) and agg not in (listify, stringify)
        ]
        if not expression_columns:
            return result
        return result.with_columns(
            pl.Series(
                col,
                # each element of a List column is a polars Series with the values in the group
                [aggs[col](values) for values in result[col]],
                strict=False,
            )
            for col in expression_columns
        )

    def do_aggregate(self, df, aggs):
        agg_exprs = [self._get_agg(col, agg, {}) for col, agg in aggs.items()]
        result = df.group_by(self.group_by, maintain_order=True).agg(*agg_exprs)
        result = self._apply_aggregation_expressions(result, aggs)
        if self.aggregation_types:
            result = result.select(
                pl.col(col).cast(MAP_TYPES[self.aggregation_types[col]]) if col in self.aggregation_types else pl.col(col)
                for col in result.columns
            )
        return result
