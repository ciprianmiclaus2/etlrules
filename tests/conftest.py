import datetime
import types

import pytest
import dask
import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends import dask as dd_rules
from etlrules.backends import pandas as pd_rules
from etlrules.backends import polars as pl_rules
from etlrules.backends import pyarrow as pa_rules


# pyarrow has no DataFrame class (it's a Table) and no Int64 type object: the tests use backend.impl.DataFrame
# and backend.impl.Int64, so the pyarrow backend fixture exposes them via this namespace
pa_impl = types.SimpleNamespace(DataFrame=pa.Table, Int64=pa.int64())


# The datetime/timedelta resolution pandas infers from python objects ("ns" in pandas 2, "us" in pandas 3)
PD_DATETIME_DTYPE = str(pd.Series([datetime.datetime(2000, 1, 1)]).dtype)
PD_TIMEDELTA_DTYPE = str(pd.Series([datetime.timedelta(days=1)]).dtype)

TYPE_MAPPING = {
    "pl": {
        "string": pl.Utf8,
        "int8": pl.Int8,
        "Int8": pl.Int8,
        "int16": pl.Int16,
        "Int16": pl.Int16,
        "int32": pl.Int32,
        "Int32": pl.Int32,
        "int64": pl.Int64,
        "Int64": pl.Int64,
        "uint8": pl.UInt8,
        "UInt8": pl.UInt8,
        "uint16": pl.UInt16,
        "UInt16": pl.UInt16,
        "uint32": pl.UInt32,
        "UInt32": pl.UInt32,
        "uint64": pl.UInt64,
        "UInt64": pl.UInt64,
        "boolean": pl.Boolean,
        "object": pl.Object,
        "float64": pl.Float64,
        "Float64": pl.Float64,
        "list_strings": pl.List(pl.Utf8),
        "list_int64s": pl.List(pl.Int64),
        "datetime": pl.Datetime,
        "timedelta": pl.Duration,
    },
    "pd": {
        "boolean": "boolean",
        "datetime": PD_DATETIME_DTYPE,
        "list_strings": "object",
        "list_int64s": "object",
        "timedelta": PD_TIMEDELTA_DTYPE,
    },
    "pa": {
        "string": pa.string(),
        "int8": pa.int8(),
        "Int8": pa.int8(),
        "int16": pa.int16(),
        "Int16": pa.int16(),
        "int32": pa.int32(),
        "Int32": pa.int32(),
        "int64": pa.int64(),
        "Int64": pa.int64(),
        "uint8": pa.uint8(),
        "UInt8": pa.uint8(),
        "uint16": pa.uint16(),
        "UInt16": pa.uint16(),
        "uint32": pa.uint32(),
        "UInt32": pa.uint32(),
        "uint64": pa.uint64(),
        "UInt64": pa.uint64(),
        "boolean": pa.bool_(),
        "float32": pa.float32(),
        "Float32": pa.float32(),
        "float64": pa.float64(),
        "Float64": pa.float64(),
        "list_strings": pa.list_(pa.string()),
        "list_int64s": pa.list_(pa.int64()),
        "datetime": pa.timestamp("us"),
        "timedelta": pa.duration("us"),
    },
    "dd": {
        "boolean": "boolean",
        "datetime": PD_DATETIME_DTYPE,
        "list_strings": "object",
        "list_int64s": "object",
        "timedelta": PD_TIMEDELTA_DTYPE,
    }
}

class BackendFixture:
    def __init__(self, name, impl_pckg, rules_pckgs):
        self.name = name
        self.impl_pckg = impl_pckg
        self.rules_pckgs = rules_pckgs

    @property
    def impl(self):
        return self.impl_pckg

    @property
    def rules(self):
        return self.rules_pckgs

    def __str__(self):
        return self.name

    def DataFrame(self, data, dtype=None, astype=None):

        def get_data_keys(ddata):
            keys = []
            if isinstance(ddata, list):
                for elem in ddata:
                    assert isinstance(elem, dict)
                    for k in elem.keys():
                        if k not in keys:
                            keys.append(k)
            elif isinstance(ddata, dict):
                keys = list(ddata.keys())
            return keys

        if self.impl_pckg == pd:
            df = pd.DataFrame(data, dtype=TYPE_MAPPING["pd"].get(dtype, dtype))
            if astype is not None:
                df = df.astype({k: TYPE_MAPPING["pd"].get(v, v) for k, v in astype.items()})
        elif self.impl_pckg == dd:
            df = pd.DataFrame(data, dtype=TYPE_MAPPING["pd"].get(dtype, dtype))
            if astype is not None:
                astype = {k: TYPE_MAPPING["pd"].get(v, v) for k, v in astype.items()}
                # Convert in pandas first: with dask's dataframe.convert-string enabled (the default),
                # from_pandas converts all object columns (e.g. bools mixed with None) to strings.
                df = df.astype(astype)
            if any(isinstance(val, (list, tuple)) for col in df.columns for val in df[col] if df[col].dtype == object):
                # dask's dataframe.convert-string (the default) would turn the lists into their string representation
                with dask.config.set({"dataframe.convert-string": False}):
                    df = dd.from_pandas(df, npartitions=1)
            else:
                df = dd.from_pandas(df, npartitions=1)
            if astype is not None:
                # Convert again on the dask side: dask's tokenize ignores the dtype of pandas
                # masked arrays, so from_pandas of two frames with the same values but e.g. Int8 vs Int64
                # would resolve to the same (cached) dask expression.
                df = df.astype(astype)
        elif self.impl_pckg == pa_impl:
            keys = get_data_keys(data)
            types_ = {}
            if dtype is not None:
                types_ = {k: TYPE_MAPPING["pa"][dtype] for k in keys}
            elif astype is not None:
                types_ = {k: TYPE_MAPPING["pa"][v] for k, v in astype.items() if k in keys}
            if isinstance(data, dict):
                columns = {k: data[k] for k in keys}
            else:
                columns = {k: [row.get(k) for row in data] for k in keys}
            def to_array(values, type_):
                try:
                    return pa.array(values, type=type_)
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    if type_ is not None:
                        raise
                    # mixed types (e.g. strings and floats): convert to strings like polars does
                    return pa.array([None if v is None else str(v) for v in values], type=pa.string())
            df = pa.table({k: to_array(values, types_.get(k)) for k, values in columns.items()})
        elif self.impl_pckg == pl:
            schema = None
            if dtype is not None:
                keys = get_data_keys(data)
                if keys:
                    schema = {k: TYPE_MAPPING["pl"][dtype] for k in keys}
            elif astype is not None:
                assert isinstance(astype, dict)
                keys = get_data_keys(data)
                schema = {
                    col: TYPE_MAPPING["pl"][astype[col]] if col in astype else None for col in keys
                }
            df = pl.DataFrame(data, schema)
        else:
            assert False, f"unknown impl_pckg: {self.impl_pckg}"
        return df

    def astype(self, df, astype):
        if self.impl_pckg == pd:
            astype = {
                col: TYPE_MAPPING["pd"][dtype] for col, dtype in astype.items()
            }
            return df.astype(astype)
        elif self.impl_pckg == dd:
            astype = {
                col: TYPE_MAPPING["pd"][dtype] for col, dtype in astype.items()
            }
            return df.astype(astype)
        elif self.impl_pckg == pa_impl:
            for col, dtype in astype.items():
                column = pc.cast(df[col], TYPE_MAPPING["pa"][dtype])
                df = df.set_column(df.column_names.index(col), col, column)
            return df
        elif self.impl_pckg == pl:
            def cast(col, dtype):
                pl_type = TYPE_MAPPING["pl"][dtype]
                if df.schema[col] == pl.Utf8 and pl_type == pl.Datetime:
                    # polars doesn't support casting strings to datetime
                    return pl.col(col).str.to_datetime(time_unit="us")
                return pl.col(col).cast(pl_type)
            return df.with_columns(
                *[cast(col, dtype) for col, dtype in astype.items()]
            )
        assert False, f"unknown impl_pckg: {self.impl_pckg}"

    def assign(self, df, column_name, column):
        if self.impl_pckg == pa_impl:
            return df.append_column(column_name, column if isinstance(column, (pa.Array, pa.ChunkedArray)) else pa.array(column))
        if self.impl_pckg == pl:
            return df.with_columns(**{column_name: column})
        return df.assign(**{column_name: column})

    def rename(self, df, rename_dict):
        if self.impl_pckg == pd:
            return df.rename(columns=rename_dict)
        elif self.impl_pckg == dd:
            return df.rename(columns=rename_dict)
        elif self.impl_pckg == pa_impl:
            columns = list(df.column_names)
            return df.rename_columns([
                rename_dict.get(idx, rename_dict.get(col, col)) for idx, col in enumerate(columns)
            ])
        elif self.impl_pckg == pl:
            columns = list(df.columns)
            if all(isinstance(key, int) and 0 <= key < len(columns) for key in rename_dict.keys()):
                rename_dict = {
                    columns[key]: val for key, val in rename_dict.items()
                }
            return df.rename(rename_dict)
        assert False, f"unknown impl_pckg: {self.impl_pckg}"

    def hconcat(self, df1, df2):
        if self.impl_pckg == pd:
            return pd.concat((df1, df2), axis=1)
        elif self.impl_pckg == dd:
            return dd.concat([df1, df2], axis=1)
        elif self.impl_pckg == pl:
            return df1.hstack(df2, in_place=False)
        elif self.impl_pckg == pa_impl:
            for name in df2.column_names:
                df1 = df1.append_column(name, df2[name])
            return df1
        assert False, f"unknown impl_pckg: {self.impl_pckg}"

    def empty_df(self, df):
        if self.impl_pckg in (pd, pl):
            return df[:0]
        elif self.impl_pckg == pa_impl:
            return df.slice(0, 0)
        elif self.impl_pckg == dd:
            return dd.from_pandas(df.head(0), npartitions=1)
        assert False, f"unknown impl_pckg: {self.impl_pckg}"

    def business_day_offset(self, dt_col, offset, strict=True):
        if self.impl_pckg == pd:
            from etlrules.backends.pandas.datetime import business_day_offset
            return business_day_offset(dt_col, offset, strict=strict)
        elif self.impl_pckg == dd:
            from etlrules.backends.dask.datetime import business_day_offset
            return business_day_offset(dt_col, offset, strict=strict)
        elif self.impl_pckg == pl:
            from etlrules.backends.polars.datetime import business_day_offset
            return business_day_offset(dt_col, offset, strict=strict)
        elif self.impl_pckg == pa_impl:
            from etlrules.backends.pyarrow.datetime import business_day_offset
            return business_day_offset(dt_col, offset, strict=strict)
        assert False, "Not implemented for backend."

    def months_offset(self, dt_col, offset, strict=True):
        if self.impl_pckg == pd:
            from etlrules.backends.pandas.datetime import months_offset
            return months_offset(dt_col, offset, strict=strict)
        elif self.impl_pckg == dd:
            from etlrules.backends.dask.datetime import months_offset
            return months_offset(dt_col, offset, strict=strict)
        elif self.impl_pckg == pl:
            from etlrules.backends.polars.datetime import months_offset
            return months_offset(dt_col, offset, strict=strict)
        elif self.impl_pckg == pa_impl:
            from etlrules.backends.pyarrow.datetime import months_offset
            return months_offset(dt_col, offset, strict=strict)
        assert False, "Not implemented for backend."

@pytest.fixture(params=[
    ('pandas', pd, pd_rules),
    ('polars', pl, pl_rules),
    ('dask', dd, dd_rules),
    ('pyarrow', pa_impl, pa_rules),
])
def backend(request):
    return BackendFixture(*request.param)
