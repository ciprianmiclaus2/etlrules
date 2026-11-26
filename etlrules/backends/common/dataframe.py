""" Helpers for the dataframe operations used by the backend independent (common) rule logic.

Most backends (pandas, polars, dask) expose the column names as `df.columns` and select a subset of columns
with `df[list_of_columns]`. pyarrow tables expose the column names as `table.column_names` (`table.columns`
returns the columns' data) and select columns with `table.select(...)`.
"""
from typing import Sequence

import pyarrow as pa


def get_column_names(df) -> list:
    """ Returns the list of column names of a dataframe (of any of the supported backends). """
    if isinstance(df, pa.Table):
        return list(df.column_names)
    return list(df.columns)


def select_columns(df, columns: Sequence[str]):
    """ Returns a dataframe with only the given columns, in the given order. """
    if isinstance(df, pa.Table):
        return df.select(list(columns))
    return df[list(columns)]
