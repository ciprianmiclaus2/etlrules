from pandas import isnull

from etlrules.backends.common.aggregate import AggregateRule as AggregateRuleBase


class AggregateRule(AggregateRuleBase):

    AGGREGATIONS = {
        "min": "min",
        "max": "max",
        "mean": "mean",
        "count": "size",
        "countNoNA": "count",
        "sum": "sum",
        "first": "first",
        "last": "last",
        "list": lambda values: [value for value in values if not isnull(value)],
        "tuple": lambda values: tuple(value for value in values if not isnull(value)),
        "csv": lambda values: ",".join(
            str(elem) for elem in values if not isnull(elem)
        ),
    }

    def do_aggregate(self, df, aggs):
        return df.groupby(by=self.group_by, as_index=False, dropna=False).agg(aggs)
