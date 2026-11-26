import os
import tempfile

import dask.dataframe as dd
import pandas as pd

from etlrules.backends.common.io.db import (
    ReadSQLQueryRule as ReadSQLQueryRuleBase,
    WriteSQLTableRule as WriteSQLTableRuleBase,
    SQL_ERRORS,
)
from etlrules.backends.dask.types import MAP_TYPES
from etlrules.data import context
from etlrules.exceptions import SQLError


def _read_batches(files, dtypes):
    # Every partition is converted to the column types collected from all the batches: a batch where a column
    # has only nulls doesn't have the column's type (e.g. Int64) and would otherwise read back as object.
    df = dd.read_parquet(files)
    meta = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in dtypes.items()})
    return df.map_partitions(lambda part, part_dtypes: part.astype(part_dtypes), dtypes, meta=meta)


class ReadSQLQueryRule(ReadSQLQueryRuleBase):

    def _do_apply(self, connection):
        if self.column_types:
            column_types = {col: MAP_TYPES[col_type] for col, col_type in self.column_types.items()}
        else:
            column_types = None

        import sqlalchemy as sa
        res = connection.execution_options(stream_results=True).execute(
            sa.text(self._get_sql_query())
        )
        keys = res.keys()
        temp_dir = tempfile.mkdtemp(prefix='dask_sql_read', dir=context.etlrules_tempdir)
        files = []  # in the order of the results
        dtypes = {}
        for idx, partition in enumerate(res.partitions(self.batch_size)):
            data = [dict(zip(keys, row)) for row in partition]
            # same types as the pandas backend: nullable types (e.g. Int64 for ints with nulls, not float64)
            df = pd.DataFrame(data=data).convert_dtypes()
            if column_types:
                df = df.astype(column_types)
            for col, dtype in df.dtypes.items():
                if dtypes.get(col, object) == object:
                    dtypes[col] = dtype
            file_path = os.path.join(temp_dir, f'data-{idx}.parquet')
            df.to_parquet(path=file_path, index=False)
            files.append(file_path)
        if not files:
            # no results, try our best to construct an empty df
            df = pd.DataFrame(data={k: [] for k in keys})
            if column_types:
                df = df.astype(column_types)
            return dd.from_pandas(df, npartitions=1)
        return _read_batches(files, dtypes)


class WriteSQLTableRule(WriteSQLTableRuleBase):

    METHOD = 'multi'

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        try:
            df.to_sql(
                self._get_sql_table(),
                self._get_sql_engine(),
                if_exists=self.if_exists,
                index=False,
                method=self.METHOD
            )
        except SQL_ERRORS as exc:
            raise SQLError(str(exc))
