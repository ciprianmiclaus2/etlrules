import polars as pl

from etlrules.backends.common.aggregate import AggregateRule as AggregateRuleBase
from etlrules.backends.polars.types import MAP_TYPES


class AggregateRule(AggregateRuleBase):

    AGGREGATIONS = {
        "min": "min",
        "max": "max",
        "mean": "mean",
        "count": "count",
        "countNoNA": ["is_not_null", "sum"],
        "sum": "sum",
        "first": "first",
        "last": "last",
        "list": lambda values: [value for value in values if value is not None],
        "csv": lambda values: ",".join(str(elem) for elem in values if elem is not None),
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
        return pl.col(col).map_elements(aggs)

    def do_aggregate(self, df, aggs):
        aggs = [self._get_agg(col, aggs) for col, aggs in aggs.items()]
        result = df.group_by(by=self.group_by, maintain_order=True).agg(*aggs)
        if self.aggregation_types:
            result = result.select(
                pl.col(col).cast(MAP_TYPES[self.aggregation_types[col]]) if col in self.aggregation_types else pl.col(col)
                for col in result.columns
            )
        return result
