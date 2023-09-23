from finrules.exceptions import MissingColumn, UnsupportedType
from finrules.rule import UnaryOpBaseRule


class TypeConversionRule(UnaryOpBaseRule):
    """ Converts the type of a given set of columns to other types.

    Args:
        mapper: A dict with columns names as keys and the new types as values.

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
        MissingColumn is raised when a column specified in the mapper doesn't exist in the input data frame.
        UnsupportedType is raised when an unknown type is speified in the values of the mapper.
    """

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
        super().apply(data)
        df = self._get_input_df(data)
        columns_set = set(df.columns)
        for column_name, type_str in self.mapper.items():
            if column_name not in columns_set:
                raise MissingColumn(f"Column '{column_name}' is missing in the data frame. Available columns: {sorted(columns_set)}")
            if type_str not in self.SUPPORTED_TYPES:
                raise UnsupportedType(f"Type '{type_str}' for column '{column_name}' is not currently supported.")
            df[column_name] = df[column_name].astype(type_str)
        self._set_output_df(data, df)
