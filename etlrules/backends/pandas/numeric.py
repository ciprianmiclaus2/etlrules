from typing import Iterable, Optional, Sequence, Union

from etlrules.backends.pandas.validation import ColumnsInOutMixin
from etlrules.rule import UnaryOpBaseRule


class RoundRule(UnaryOpBaseRule, ColumnsInOutMixin):
    """ Rounds a set of columns to specified decimal places.

    Basic usage::

        # rounds Col_A, Col_B and Col_C to 2dps
        rule = RoundRule(["Col_A", "Col_B", "Col_C"], 2)
        rule.apply(data)

        # rounds Col_A to 2dps, Col_B to 0dps and Col_C to 4dps
        rule = RoundRule(["Col_A", "Col_B", "Col_C"], [2, 0, 4])
        rule.apply(data)

    Args:
        columns: A list of columns to round as per the specified scale.
        scale: Either an integer specifying a number of decimal places to round to, which applies to all columns,
            or a list/tuple of integers specifying the number of decimal places to round each column.
            In the latter case, the length of the scale list/tuple must match the length of the columns list.
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the rounded values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the lower case values.
            If not provided, the result is updated in place.

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

    def __init__(self, columns: Iterable[str], scale: Union[int, Sequence[int]], output_columns: Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        self.output_columns = [out_col for out_col in output_columns] if output_columns else None
        self.scale = scale
        if isinstance(self.scale, int):
            self._scales = [self.scale] * len(self.columns)
        elif isinstance(self.scale, (list, tuple)):
            self._scales = self.scale
        else:
            assert False, "The scale parameter must be either an integer or a list/tuple of integers."

    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(df, self.columns, self.output_columns, self.strict)
        res_df = df.round(dict(zip(columns, self._scales)))
        df = df.assign(**{out_col: res_df[col] for col, out_col in zip(columns, output_columns)})
        self._set_output_df(data, df)


class AbsRule(UnaryOpBaseRule, ColumnsInOutMixin):
    """ Converts numbers to absolute values.

    Basic usage::

        rule = AbsRule(["col_A", "col_B", "col_C"])
        rule.apply(data)

    Args:
        columns: A list of numeric columns to convert to absolute values.
        output_columns: A list of new names for the columns with the absolute values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the absolute values.
            If not provided, the result is updated in place.

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
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.

    Note:
        In non-strict mode, missing columns are ignored.
    """

    def __init__(self, columns: Iterable[str], output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        self.output_columns = [out_col for out_col in output_columns] if output_columns else None

    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(df, self.columns, self.output_columns, self.strict)
        abs_df = df[columns].abs()
        df = df.assign(**{output_col: abs_df[col] for col, output_col in zip(columns, output_columns)})
        self._set_output_df(data, df)
