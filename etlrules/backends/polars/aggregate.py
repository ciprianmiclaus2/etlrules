import polars as pl

from etlrules.backends.common.aggregate import AggregateRule as AggregateRuleBase


class AggregateRule(AggregateRuleBase):

    AGGREGATIONS = {
        "min": "min",
        "max": "max",
        "mean": "mean",
        "count": "size",
        "countNoNA": ["is_not_null", "sum"],
        "sum": "sum",

        "first": "first",
        "last": "last",

        "list": lambda values: [value for value in values],
        "tuple": lambda values: tuple(value for value in values),
        "csv": lambda values: ",".join(str(elem) for elem in values),
    }

    def _get_agg(self, col, aggs):
        if isinstance(aggs, str):
            return getattr(pl.col(col), aggs)()
        elif isinstance(aggs, list):
            expr = None
            for agg in aggs:
                if expr is None:
                    expr = self._get_agg(col, agg)
                else:
                    expr = getattr(expr, agg)()
            return expr
        # assumes lambda
        return pl.col(col).apply(aggs)

    def do_aggregate(self, df, aggs):
        try:
            aggs = [self._get_agg(col, aggs) for col, aggs in aggs.items()]
        except TypeError:
            breakpoint()
        return df.group_by(by=self.group_by).agg(*aggs)
