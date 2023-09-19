from finrules.exceptions import MissingColumn, UnsupportedType
from finrules.rule import UnaryOpBaseRule


class TypeConversionRule(UnaryOpBaseRule):

    SUPPORTED_TYPES = {
        'int32',
        'int64',
        'float64',
        'str',
    }

    def __init__(self, mapper, named_input=None, named_output=None, name=None, description=None, strict=True):
        assert isinstance(mapper, dict), "mapper needs to be a dict {column_name:type}"
        assert all(isinstance(key, str) and isinstance(val, str) for key, val in mapper.items()), "mapper needs to be a dict {column_name:type} where the names are str"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.mapper = mapper

    def apply(self, data):
        df = self._get_input_df(data)
        columns_set = set(df.columns)
        for column_name, type_str in self.mapper.items():
            if column_name not in columns_set:
                raise MissingColumn(f"Column '{column_name}' is missing in the data frame. Available columns: {sorted(columns_set)}")
            if type_str not in self.SUPPORTED_TYPES:
                raise UnsupportedType(f"Type '{type_str}' for column '{column_name}' is not currently supported.")
            df[column_name] = df[column_name].astype(type_str)
        self._set_output_df(data, df)
