import re

import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.basic import (
    DedupeRule as DedupeRuleBase,
    ExplodeValuesRule as ExplodeValuesRuleBase,
    RenameRule as RenameRuleBase,
    ReplaceRule as ReplaceRuleBase,
    SortRule as SortRuleBase,
)
from etlrules.backends.pyarrow.base import PyArrowMixin, ROW_INDEX_COLUMN, rename_columns, set_column, with_row_index
from etlrules.backends.pyarrow.types import MAP_TYPES


def sorted_take(table: pa.Table, row_indices) -> pa.Table:
    """ Takes the rows at row_indices, keeping the order of the rows in table. """
    row_indices = pc.take(row_indices, pc.sort_indices(row_indices))
    return table.take(row_indices)


class DedupeRule(DedupeRuleBase):

    def do_dedupe(self, df):
        indexed = with_row_index(df)
        aggregated = indexed.group_by(self.columns, use_threads=False).aggregate([
            (ROW_INDEX_COLUMN, "min"), (ROW_INDEX_COLUMN, "max"), (ROW_INDEX_COLUMN, "count"),
        ])
        if self.keep == DedupeRule.KEEP_NONE:
            rows = pc.filter(aggregated[f"{ROW_INDEX_COLUMN}_min"], pc.equal(aggregated[f"{ROW_INDEX_COLUMN}_count"], 1))
        elif self.keep == DedupeRule.KEEP_FIRST:
            rows = aggregated[f"{ROW_INDEX_COLUMN}_min"]
        else:
            rows = aggregated[f"{ROW_INDEX_COLUMN}_max"]
        return sorted_take(df, rows)


class RenameRule(RenameRuleBase):

    def do_rename(self, df, mapper):
        return rename_columns(df, mapper)


class SortRule(SortRuleBase):

    def do_sort(self, df):
        if isinstance(self.ascending, bool):
            ascending = [self.ascending] * len(self.sort_by)
        else:
            ascending = self.ascending
        return df.sort_by([(col, "ascending" if asc else "descending") for col, asc in zip(self.sort_by, ascending)])


class ReplaceRule(ReplaceRuleBase, PyArrowMixin):

    def _get_old_new_regex(self, old_val, new_val):
        # the replacement refers to groups as \1 .. \9: convert $name, ${name}, \g<name>, $1, ${1}, \g<1>
        compiled = re.compile(old_val)
        groups = {name: idx for name, idx in compiled.groupindex.items()}
        groups.update({str(idx): idx for idx in range(1, compiled.groups + 1)})
        # longest names first, so that e.g. $10 is not replaced as $1 followed by 0
        for name in sorted(groups, key=len, reverse=True):
            for ref in (f"${{{name}}}", f"\\g<{name}>", f"${name}"):
                new_val = new_val.replace(ref, f"\\{groups[name]}")
        return old_val, new_val

    def do_apply(self, df, col):
        if len(col) == 0:
            return col
        if self.regex:
            for old_val, new_val in zip(self.values, self.new_values):
                old_val, new_val = self._get_old_new_regex(old_val, new_val)
                col = pc.replace_substring_regex(col, pattern=old_val, replacement=new_val)
            return col
        values = pa.array(self.values, type=col.type)
        new_values = pa.array(self.new_values)
        if new_values.type != col.type:
            try:
                new_values = pc.cast(new_values, col.type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                pass
        positions = pc.index_in(col, value_set=values)
        return pc.if_else(pc.is_valid(positions), pc.take(new_values, positions), col)


class ExplodeValuesRule(ExplodeValuesRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        self._validate_input_column(df)
        col = df[self.input_column].combine_chunks()
        # like pandas, a null or an empty list produces one row with a null value
        empty = pc.fill_null(pc.equal(pc.list_value_length(col), 0), True)
        col = pc.if_else(empty, pa.scalar([None], type=col.type), col)
        values = pc.list_flatten(col)
        rows = pc.list_parent_indices(col)
        result = set_column(df.take(rows), self.input_column, values)
        if self.column_type:
            result = set_column(result, self.input_column, pc.cast(result[self.input_column], MAP_TYPES[self.column_type]))
        self._set_output_df(data, result)
