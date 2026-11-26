from typing import Any, Mapping

import pyarrow as pa


ROW_INDEX_COLUMN = "__etlrules_row_index"


def to_column(value: Any, num_rows: int, type: pa.DataType = None) -> pa.ChunkedArray:
    """ Converts a scalar, list, array or chunked array to a chunked array of num_rows elements. """
    if isinstance(value, pa.ChunkedArray):
        return value
    if isinstance(value, pa.Array):
        return pa.chunked_array([value])
    if isinstance(value, pa.Scalar):
        value = value.as_py()
    if isinstance(value, (list, tuple)):
        return pa.chunked_array([pa.array(value, type=type)])
    scalar = pa.scalar(value, type=type)
    return pa.chunked_array([pa.array([value] * num_rows, type=scalar.type)])


def set_column(table: pa.Table, name: str, value: Any) -> pa.Table:
    """ Replaces the column name in table if it exists (keeping its position) or appends it at the end. """
    column = to_column(value, table.num_rows)
    if name in table.column_names:
        return table.set_column(table.column_names.index(name), name, column)
    return table.append_column(name, column)


def rename_columns(table: pa.Table, mapper: Mapping[str, str]) -> pa.Table:
    # pyarrow < 17 doesn't accept a dict in rename_columns
    return table.rename_columns([mapper.get(col, col) for col in table.column_names])


def with_row_index(table: pa.Table, name: str = ROW_INDEX_COLUMN) -> pa.Table:
    return table.append_column(name, pa.array(range(table.num_rows), type=pa.int64()))


class PyArrowMixin:

    def assign_do_apply(self, df: pa.Table, input_column: str, output_column: str) -> pa.Table:
        return set_column(df, output_column, self.do_apply(df, df[input_column]))

    def assign_do_apply_dict(self, df: pa.Table, mapper_dict: Mapping[str, Any]) -> pa.Table:
        for column_name, column in mapper_dict.items():
            df = set_column(df, column_name, column)
        return df
