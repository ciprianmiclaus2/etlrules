import dask.dataframe as dd

from etlrules.backends.common.types import TypeConversionRule as TypeConversionRuleBase

from etlrules.backends.dask.base import DaskMixin


MAP_TYPES = {
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'uint8': 'UInt8',
    'uint16': 'UInt16',
    'uint32': 'UInt32',
    'uint64': 'UInt64',
    'float32': 'Float32',
    'float64': 'Float64',
    'string': 'string',
    'boolean': 'boolean',
}

NUMERIC_TYPES = {
    'int8', 'int16', 'int32', 'int64',
    'uint8', 'uint16', 'uint32', 'uint64',
    'float32', 'float64',
}


class TypeConversionRule(TypeConversionRuleBase, DaskMixin):

    def do_type_conversion(self, df, col, dtype):
        if not self.strict:
            casting = "unsafe"
        else:
            casting = "same_kind"
        return col.astype(MAP_TYPES[dtype])
