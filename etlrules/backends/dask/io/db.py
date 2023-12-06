import dask.dataframe as dd
import os
import pandas as pd
import tempfile

from etlrules.backends.common.io.db import (
    ReadSQLQueryRule as ReadSQLQueryRuleBase,
    WriteSQLTableRule as WriteSQLTableRuleBase,
)
from etlrules.backends.dask.types import MAP_TYPES
from etlrules.data import context
from etlrules.exceptions import SQLError


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
        generated = 0
        for idx, partition in enumerate(res.partitions(self.batch_size)):
            data = [dict(zip(keys, row)) for row in partition]
            df = pd.DataFrame(data=data)
            if column_types:
                df = df.astype(column_types)
            df.to_parquet(path=os.path.join(temp_dir, f'data-{idx}.parquet'), index=False)
            generated += 1
        if not generated:
            # no results, try our best to construct an empty df
            df = pd.DataFrame(data={k: [] for k in keys})
            if column_types:
                df = df.astype(column_types)
            return dd.from_pandas(df, npartitions=1)
        return dd.read_parquet(os.path.join(temp_dir, 'data-*.parquet'), backend_dtype="numpy_nullable")


class WriteSQLTableRule(WriteSQLTableRuleBase):

    METHOD = 'multi'

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        import sqlalchemy as sa
        try:
            df.to_sql(
                self._get_sql_table(),
                self._get_sql_engine(),
                if_exists=self.if_exists,
                index=False,
                method=self.METHOD
            )
        except sa.exc.SQLAlchemyError as exc:
            raise SQLError(str(exc))
