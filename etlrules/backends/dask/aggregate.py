import dask.dataframe as dd
import itertools
from pandas import isnull
from typing import Iterable, Mapping, Optional

from etlrules.backends.common.aggregate import AggregateRule as AggregateRuleBase
from etlrules.backends.dask.types import MAP_TYPES


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
        "list": dd.Aggregation(
            'list',
            chunk=lambda v: v.apply(lambda v: [value for value in v if not isnull(value)]),
            agg=lambda v: v.apply(lambda v: [value for value in itertools.chain(*v) if not isnull(value)]),
        ),
        "csv": dd.Aggregation(
            'csv',
            chunk=lambda v: v.apply(lambda v: [value for value in v if not isnull(value)]),
            agg=lambda v: v.apply(lambda values: ",".join(str(elem) for elem in itertools.chain(*values) if not isnull(elem))),
        ),
    }

    def __init__(
        self,
        group_by: Iterable[str],
        aggregations: Optional[Mapping[str, str]] = None,
        aggregation_expressions: Optional[Mapping[str, str]] = None,
        aggregation_types: Optional[Mapping[str, str]] = None,
        named_input: Optional[str] = None,
        named_output: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        strict: bool = True,
    ):
        if aggregation_expressions is not None:
            raise NotImplementedError("The dask backend doesn't support aggregation expressions")
        super().__init__(
            group_by, aggregations, aggregation_expressions, aggregation_types,
            named_input, named_output, name, description, strict)

    def do_aggregate(self, df, aggs):
        result = df.groupby(by=self.group_by, dropna=False).agg(aggs).reset_index()
        if self.aggregation_types:
            result = result.astype({col: MAP_TYPES[col_type] for col, col_type in self.aggregation_types.items()})
        return result
