from typing import Iterable, Mapping, Optional

from etlrules.exceptions import MissingColumnError
from etlrules.rule import UnaryOpBaseRule


class RoundRule(UnaryOpBaseRule):
    """ Rounds a set of columns to specified decimal places.

    Basic usage::

        # rounds Col_A to 2dps, Col_B to 0dps and Col_C to 4dps
        rule = RoundRule({"Col_A": 2, "Col_B": 0, "Col_C": 4})
        rule.apply(data)

    Args:
        mapper: A dict {column_name: scale} which specifies to round each column to the
            number of decimal places specified in the scale.

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
        MissingColumnError: raised in strict mode only if a column in the mapper doesn't exist in the input dataframe.

    Note:
        In non-strict mode, missing columns are ignored.
    """

    def __init__(self, mapper: Mapping[str, int], named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert all(isinstance(col, str) and isinstance(scale, (int, float)) and int(scale) >= 0 for col, scale in mapper.items()), "Mapper is a {column_name: precision} where column names are strings and precision is an int or float and >=0."
        self.mapper = {col: int(scale) for col, scale in mapper.items()}

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict:
            if not set(self.mapper.keys()) <= set(df.columns):
                raise MissingColumnError(f"Column(s) {set(self.mapper.keys()) - set(df.columns)} are missing from the input dataframe.")
            mapper = self.mapper
        else:
            mapper = {col: scale for col, scale in self.mapper.items() if col in df.columns}
        df = df.round(mapper)
        self._set_output_df(data, df)


class AbsRule(UnaryOpBaseRule):
    """ Converts numbers to absolute values.

    Basic usage::

        # rounds Col_A to 2dps, Col_B to 0dps and Col_C to 4dps
        rule = AbsRule(["col_A", "col_B", "col_C"])
        rule.apply(data)

    Args:
        columns: A list of numeric columns to convert to absolute values.

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
        MissingColumnError: raised in strict mode only if a column in the mapper doesn't exist in the input dataframe.

    Note:
        In non-strict mode, missing columns are ignored.
    """

    def __init__(self, columns: Iterable[str], named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]

    def apply(self, data):
        df = self._get_input_df(data)
        df_cols_set = set(df.columns)
        if self.strict:
            if not set(self.columns) <= df_cols_set:
                raise MissingColumnError(f"Column(s) {set(self.columns) - df_cols_set} are missing from the input dataframe.")
            columns = self.columns
        else:
            columns = [col for col in self.columns if col in df_cols_set]
        abs_df = df[columns].abs()
        df = df.assign(**{col: abs_df[col] for col in columns})
        self._set_output_df(data, df)
