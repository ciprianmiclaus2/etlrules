import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.fill import BackFillRule as BackFillRuleBase, ForwardFillRule as ForwardFillRuleBase


def _grouped_fill(table: pa.Table, columns, group_by, forward: bool) -> pa.Table:
    """ Fills the nulls in columns with values from the same group (group_by columns), keeping the row order. """
    # a stable sort by the group columns keeps the original order of the rows within each group
    order = pc.sort_indices(table, sort_keys=[(col, "ascending") for col in group_by])
    grouped = table.take(order)
    row_numbers = pa.array(range(grouped.num_rows), type=pa.int64())
    fill_nulls = pc.fill_null_forward if forward else pc.fill_null_backward
    for col in columns:
        values = grouped[col]
        # the row each value would be filled from
        source = fill_nulls(pc.if_else(pc.is_valid(values), row_numbers, pa.scalar(None, pa.int64())))
        source_or_self = pc.fill_null(source, row_numbers)
        same_group = None
        for key in group_by:
            equal = pc.fill_null(pc.equal(pc.take(grouped[key], source_or_self), grouped[key]), False)
            same_group = equal if same_group is None else pc.and_(same_group, equal)
        filled = pc.if_else(same_group, pc.take(values, source_or_self), values)
        grouped = grouped.set_column(grouped.column_names.index(col), col, filled)
    # back to the original order of the rows
    return grouped.take(pc.sort_indices(order))


class FillMixin:

    FORWARD = None

    def do_apply(self, df):
        if self.sort_by:
            order = "ascending" if self.sort_ascending else "descending"
            df = df.sort_by([(col, order) for col in self.sort_by])
        if self.group_by:
            return _grouped_fill(df, self.columns, self.group_by, forward=self.FORWARD)
        fill_nulls = pc.fill_null_forward if self.FORWARD else pc.fill_null_backward
        for col in self.columns:
            df = df.set_column(df.column_names.index(col), col, fill_nulls(df[col]))
        return df


class ForwardFillRule(FillMixin, ForwardFillRuleBase):
    FILL_METHOD = "forward_fill"
    FORWARD = True


class BackFillRule(FillMixin, BackFillRuleBase):
    FILL_METHOD = "backward_fill"
    FORWARD = False
