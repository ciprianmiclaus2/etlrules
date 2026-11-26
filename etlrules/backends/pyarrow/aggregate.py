import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.aggregate import AggregateRule as AggregateRuleBase
from etlrules.backends.pyarrow.types import MAP_TYPES


def drop_nulls_in_lists(lists) -> pa.Array:
    """ Removes the null values inside each list (there's no list_drop_null compute function). """
    if isinstance(lists, pa.ChunkedArray):
        lists = lists.combine_chunks()
    values = pc.list_flatten(lists)
    valid = pc.is_valid(values)
    # the new offsets are the number of valid values before each list
    valid_before = pa.concat_arrays([
        pa.array([0], type=pa.int32()),
        pc.cast(pc.cumulative_sum(pc.cast(valid, pa.int32())), pa.int32()),
    ])
    offsets = pc.subtract(lists.offsets, lists.offsets[0])
    new_offsets = pc.take(valid_before, offsets)
    return pa.ListArray.from_arrays(new_offsets, pc.filter(values, valid), mask=pc.is_null(lists))


def list_aggregation(lists):
    return drop_nulls_in_lists(lists)


def csv_aggregation(lists):
    lists = drop_nulls_in_lists(lists)
    lists = pc.cast(lists, pa.list_(pa.string()))
    return pc.binary_join(lists, ",")


class AggregateRule(AggregateRuleBase):

    AGGREGATIONS = {
        "min": ("min", None, None),
        "max": ("max", None, None),
        "mean": ("mean", None, None),
        "count": ("count", pc.CountOptions(mode="all"), None),
        "countNoNA": ("count", pc.CountOptions(mode="only_valid"), None),
        "sum": ("sum", pc.ScalarAggregateOptions(min_count=0), None),
        "first": ("first", None, None),
        "last": ("last", None, None),
        "list": ("list", None, list_aggregation),
        "csv": ("list", None, csv_aggregation),
    }

    def do_aggregate(self, df, aggs):
        aggregations = []
        post_processing = {}
        for col, agg in aggs.items():
            if isinstance(agg, tuple):
                func, options, post = agg
            else:
                # aggregation expression: collect the values in a list and evaluate the expression for each group
                func, options, post = "list", None, None
                post_processing[col] = agg
            aggregations.append((col, func, options) if options is not None else (col, func))
            if post is not None:
                post_processing[col] = post
        result = df.group_by(self.group_by, use_threads=False).aggregate(aggregations)
        columns = {}
        for col, func_spec in zip(aggs, aggregations):
            column = result[f"{col}_{func_spec[1]}"]
            post = post_processing.get(col)
            if post in (list_aggregation, csv_aggregation):
                column = post(column)
            elif post is not None:
                column = pa.array([post(values) for values in column.to_pylist()])
            columns[col] = column
        result = pa.table({**{col: result[col] for col in self.group_by}, **columns})
        if self.aggregation_types:
            for col, col_type in self.aggregation_types.items():
                if col in result.column_names:
                    result = result.set_column(result.column_names.index(col), col, pc.cast(result[col], MAP_TYPES[col_type]))
        return result
