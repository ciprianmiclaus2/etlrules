import pyarrow as pa

from etlrules.backends.common.concat import (
    VConcatRule as VConcatRuleBase,
    HConcatRule as HConcatRuleBase,
)


def concat_tables(tables):
    """ Vertically concatenates tables, matching the columns by name.

    Columns missing from a table are filled with nulls (the columns of the first table come first).
    """
    tables = [table for table in tables if table is not None]
    first = tables[0]
    tables = [first] + [
        table.select([col for col in first.column_names if col in table.column_names] +
                     [col for col in table.column_names if col not in first.column_names])
        for table in tables[1:]
    ]
    return pa.concat_tables(tables, promote_options="default")


def pad_rows(table: pa.Table, num_rows: int) -> pa.Table:
    """ Adds null rows at the end of the table so it has num_rows rows. """
    missing = num_rows - table.num_rows
    if missing <= 0:
        return table
    nulls = pa.table({field.name: pa.nulls(missing, type=field.type) for field in table.schema}, schema=table.schema)
    return pa.concat_tables([table, nulls])


class VConcatRule(VConcatRuleBase):

    def do_concat(self, left_df, right_df):
        return concat_tables([left_df, right_df])


class HConcatRule(HConcatRuleBase):

    def do_concat(self, left_df, right_df):
        num_rows = max(left_df.num_rows, right_df.num_rows)
        left_df, right_df = pad_rows(left_df, num_rows), pad_rows(right_df, num_rows)
        for name in right_df.column_names:
            left_df = left_df.append_column(name, right_df[name])
        return left_df
