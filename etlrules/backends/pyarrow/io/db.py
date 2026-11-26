import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.io.db import (
    ReadSQLQueryRule as ReadSQLQueryRuleBase,
    WriteSQLTableRule as WriteSQLTableRuleBase,
    SQL_ERRORS,
    SQLAlchemyEngines,
)
from etlrules.backends.pandas.types import MAP_TYPES as PANDAS_MAP_TYPES
from etlrules.backends.pyarrow.types import MAP_TYPES
from etlrules.exceptions import SQLError


# pyarrow has no SQL support: the data is read and written with pandas (and sqlalchemy)

# arrow types -> pandas nullable types, so that e.g. ints with nulls are not converted to floats
PANDAS_TYPES = {
    pa.int8(): pd.Int8Dtype(), pa.int16(): pd.Int16Dtype(), pa.int32(): pd.Int32Dtype(), pa.int64(): pd.Int64Dtype(),
    pa.uint8(): pd.UInt8Dtype(), pa.uint16(): pd.UInt16Dtype(), pa.uint32(): pd.UInt32Dtype(), pa.uint64(): pd.UInt64Dtype(),
    pa.float32(): pd.Float32Dtype(), pa.float64(): pd.Float64Dtype(),
    pa.bool_(): pd.BooleanDtype(), pa.string(): pd.StringDtype(), pa.large_string(): pd.StringDtype(),
}


def from_pandas(df: pd.DataFrame) -> pa.Table:
    table = pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(None)
    # pandas strings can be converted to large_string: use string like the rest of the backend
    for idx, field in enumerate(table.schema):
        if pa.types.is_large_string(field.type):
            table = table.set_column(idx, field.name, pc.cast(table[field.name], pa.string()))
    return table


def to_pandas(table: pa.Table) -> pd.DataFrame:
    return table.to_pandas(types_mapper=PANDAS_TYPES.get)


class ReadSQLQueryRule(ReadSQLQueryRuleBase):

    def _do_apply(self, connection):
        column_types = None
        if self.column_types is not None:
            column_types = {col: PANDAS_MAP_TYPES[col_type] for col, col_type in self.column_types.items()}
        df = pd.read_sql_query(self._get_sql_query(), connection, dtype=column_types).convert_dtypes()
        table = from_pandas(df)
        for col, col_type in (self.column_types or {}).items():
            if col in table.column_names:
                table = table.set_column(table.column_names.index(col), col, pc.cast(table[col], MAP_TYPES[col_type]))
        return table


class WriteSQLTableRule(WriteSQLTableRuleBase):

    METHOD = 'multi'

    def apply(self, data):
        super().apply(data)
        df = to_pandas(self._get_input_df(data))
        engine = SQLAlchemyEngines.get_engine(self._get_sql_engine())
        with engine.connect() as connection:
            try:
                df.to_sql(self._get_sql_table(), connection, if_exists=self.if_exists, index=False, method=self.METHOD)
            except SQL_ERRORS as exc:
                raise SQLError(str(exc))
            connection.commit()
