from typing import Mapping, Optional

from etlrules.exceptions import MissingColumnError, UnsupportedTypeError
from etlrules.rule import UnaryOpBaseRule


SUPPORTED_TYPES = {
    'int8', 'int16', 'int32', 'int64',
    'uint8', 'uint16', 'uint32', 'uint64',
    'float32', 'float64',
    'string',
    'boolean',
}


class TypeConversionRule(UnaryOpBaseRule):
    """ Converts the type of a given set of columns to other types.

    Basic usage::

        # converts column A to int64, B to string and C to datetime
        rule = TypeConversionRule({"A": "int64", "B": "string", "C": "datetime"})
        rule.apply(data)

    Args:
        mapper: A dict with columns names as keys and the new types as values.
            The supported types are: int8, int16, int32, int64, uint8, uint16,
            uint32, uint64, float32, float64, string, boolean, datetime and timedelta.

        named_input: Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output: Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name: Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict: When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised when a column specified in the mapper doesn't exist in the input data frame.
        UnsupportedTypeError: raised when an unknown type is speified in the values of the mapper.
        ValueError: raised in strict mode if a value cannot be converted to the desired type.
            In non strict mode, the exception is not raised and the value is converted to NA.
    """

    def __init__(self, mapper: Mapping[str, str], named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        assert isinstance(mapper, dict), "mapper needs to be a dict {column_name:type}"
        assert all(isinstance(key, str) and isinstance(val, str) for key, val in mapper.items()), "mapper needs to be a dict {column_name:type} where the names are str"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.mapper = mapper
        for column_name, type_str in self.mapper.items():
            if type_str not in SUPPORTED_TYPES:
                raise UnsupportedTypeError(f"Type '{type_str}' for column '{column_name}' is not currently supported.")


    def do_type_conversion(self, df, col, dtype):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        columns_set = set(df.columns)
        for column_name in self.mapper:
            if column_name not in columns_set:
                raise MissingColumnError(f"Column '{column_name}' is missing in the data frame. Available columns: {sorted(columns_set)}")
        df = self.assign_do_apply_dict(df, {
            column_name: self.do_type_conversion(df, df[column_name], type_str) 
                for column_name, type_str in self.mapper.items()
        })
        self._set_output_df(data, df)
