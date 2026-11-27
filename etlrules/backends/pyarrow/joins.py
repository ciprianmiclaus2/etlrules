import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.joins import (
    LeftJoinRule as LeftJoinRuleBase,
    InnerJoinRule as InnerJoinRuleBase,
    OuterJoinRule as OuterJoinRuleBase,
    RightJoinRule as RightJoinRuleBase,
    SemiJoinRule as SemiJoinRuleBase,
    AntiJoinRule as AntiJoinRuleBase,
)

LEFT_ROW = "__etlrules_left_row"
RIGHT_ROW = "__etlrules_right_row"

JOIN_TYPES = {
    "left": "left outer",
    "inner": "inner",
    "outer": "full outer",
    "right": "right outer",
}


class JoinsMixin:
    """ Database style joins with the same result as pandas' merge.

    The join only uses the key columns and the row numbers of the two tables. The rows of the result are then
    taken from the two tables by their row numbers: this keeps the order of the rows (like pandas) and supports
    any type of columns (pyarrow's join doesn't support list columns, for example).
    """

    def do_apply(self, left_df, right_df):
        left_on, right_on = self._get_key_columns()
        keys = [f"__etlrules_key_{idx}" for idx in range(len(left_on))]
        left_keys = pa.table({key: left_df[col] for key, col in zip(keys, left_on)}).append_column(
            LEFT_ROW, pa.array(range(left_df.num_rows), type=pa.int64()))
        right_keys = pa.table({key: right_df[col] for key, col in zip(keys, right_on)}).append_column(
            RIGHT_ROW, pa.array(range(right_df.num_rows), type=pa.int64()))
        joined = left_keys.join(right_keys, keys=keys, join_type=JOIN_TYPES[self.JOIN_TYPE], coalesce_keys=False)
        first, second = (RIGHT_ROW, LEFT_ROW) if self.JOIN_TYPE == "right" else (LEFT_ROW, RIGHT_ROW)
        joined = joined.sort_by([(first, "ascending"), (second, "ascending")])
        left_rows, right_rows = joined[LEFT_ROW], joined[RIGHT_ROW]

        # key columns with the same name on both sides produce a single column
        merged_keys = {left for left, right in zip(left_on, right_on) if left == right}
        right_columns = [col for col in right_df.column_names if col not in merged_keys]
        overlapping = set(left_df.column_names) & set(right_columns)
        suffix_left, suffix_right = self.suffixes
        columns = {}
        for col in left_df.column_names:
            values = left_df[col].take(left_rows)
            if col in merged_keys:
                values = pc.coalesce(values, right_df[col].take(right_rows))
            name = col + suffix_left if col in overlapping and suffix_left else col
            columns[name] = values
        for col in right_columns:
            name = col + suffix_right if col in overlapping and suffix_right else col
            columns[name] = right_df[col].take(right_rows)
        return pa.table(columns)


class LeftJoinRule(JoinsMixin, LeftJoinRuleBase):
    JOIN_TYPE = "left"


class InnerJoinRule(JoinsMixin, InnerJoinRuleBase):
    JOIN_TYPE = "inner"


class OuterJoinRule(JoinsMixin, OuterJoinRuleBase):
    JOIN_TYPE = "outer"


class RightJoinRule(JoinsMixin, RightJoinRuleBase):
    JOIN_TYPE = "right"


class FilterJoinMixin:
    """ Keeps (semi join) or removes (anti join) the left rows with a match in the right dataframe. """

    JOIN_TYPE = None

    def do_apply(self, left_df, right_df):
        left_on, right_on = self._get_key_columns()
        keys = [f"__etlrules_key_{idx}" for idx in range(len(left_on))]
        left_keys = pa.table({key: left_df[col] for key, col in zip(keys, left_on)}).append_column(
            LEFT_ROW, pa.array(range(left_df.num_rows), type=pa.int64()))
        right_keys = pa.table({key: right_df[col] for key, col in zip(keys, right_on)})
        # missing keys don't match in pyarrow's joins (like SQL)
        joined = left_keys.join(right_keys, keys=keys, join_type=self.JOIN_TYPE)
        rows = joined[LEFT_ROW]
        # keep the order of the left rows
        return left_df.take(pc.take(rows, pc.sort_indices(rows)))


class SemiJoinRule(FilterJoinMixin, SemiJoinRuleBase):
    JOIN_TYPE = "left semi"


class AntiJoinRule(FilterJoinMixin, AntiJoinRuleBase):
    JOIN_TYPE = "left anti"
