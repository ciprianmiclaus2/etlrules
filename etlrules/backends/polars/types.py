import polars as pl

from etlrules.backends.common.types import TypeConversionRule as TypeConversionRuleBase

from .base import PolarsMixin

MAP_TYPES = {
    'int8': pl.Int8,
    'int16': pl.Int16,
    'int32': pl.Int32,
    'int64': pl.Int64,
    'float32': pl.Float32,
    'float64': pl.Float64,
    'string': pl.Utf8,
    'datetime': pl.Datetime,
    'timedelta': pl.Duration,
}


class TypeConversionRule(TypeConversionRuleBase, PolarsMixin):

    def do_type_conversion(self, df, col, dtype):
        return col.cast(MAP_TYPES[dtype])
