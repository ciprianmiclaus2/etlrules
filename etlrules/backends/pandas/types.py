from etlrules.backends.common.types import TypeConversionRule as TypeConversionRuleBase

from .base import PandasMixin


class TypeConversionRule(TypeConversionRuleBase, PandasMixin):

    SUPPORTED_TYPES = {
        'int8': 'Int8',
        'int16': 'Int16',
        'int32': 'Int32',
        'int64': 'Int64',
        'float32': 'float32',
        'float64': 'float64',
        'string': 'string',
        'datetime': 'datetime64[ns]',
    }

    def do_type_conversion(self, df, col, dtype):
        return col.astype(self.SUPPORTED_TYPES[dtype])
