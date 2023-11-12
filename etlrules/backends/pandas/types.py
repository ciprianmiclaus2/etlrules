from etlrules.backends.common.types import TypeConversionRule as TypeConversionRuleBase

from .base import PandasMixin


MAP_TYPES = {
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'float32': 'float32',
    'float64': 'float64',
    'string': 'string',
    'datetime': 'datetime64[ns]',
    'timedelta': 'timedelta64[ns]',
}


class TypeConversionRule(TypeConversionRuleBase, PandasMixin):

    def do_type_conversion(self, df, col, dtype):
        return col.astype(MAP_TYPES[dtype])
