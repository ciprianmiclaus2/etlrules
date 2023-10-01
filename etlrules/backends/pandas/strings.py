from typing import Iterable, Mapping, Optional, Union

from etlrules.backends.pandas.validation import ColumnsInOutMixin
from etlrules.exceptions import MissingColumnError
from etlrules.rule import UnaryOpBaseRule


class BaseStrRule(UnaryOpBaseRule, ColumnsInOutMixin):

    def __init__(self, columns: Iterable[str], output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        self.output_columns = [out_col for out_col in output_columns] if output_columns else None

    def do_apply(self, col):
        raise NotImplementedError

    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(df, self.columns, self.output_columns, self.strict)
        df = df.assign(**{output_col: self.do_apply(df[col]) for col, output_col in zip(columns, output_columns)})
        self._set_output_df(data, df)


class StrLowerRule(BaseStrRule):
    """ Converts a set of string columns to lower case.

    Basic usage::

        rule = StrLowerRule(["col_A", "col_B", "col_C"])
        rule.apply(data)

    Args:
        columns: A list of string columns to convert to lower case.
        output_columns: A list of new names for the columns with the lower case values.
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
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.

    Note:
        In non-strict mode, missing columns are ignored.
    """

    def do_apply(self, col):
        return col.str.lower()


class StrUpperRule(BaseStrRule):
    """ Converts a set of string columns to upper case.

    Basic usage::

        rule = StrUpperRule(["col_A", "col_B", "col_C"])
        rule.apply(data)

    Args:
        columns: A list of string columns to convert to upper case.
        output_columns: A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
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

    def do_apply(self, col):
        return col.str.upper()


class StrCapitalizeRule(BaseStrRule):
    """ Converts a set of string columns to capitalize.

    Capitalization will convert the first letter in the string to upper case and the rest of the letters
    to lower case.

    Basic usage::

        rule = StrCapitalizeRule(["col_A", "col_B", "col_C"])
        rule.apply(data)

    Args:
        columns: A list of string columns to convert to capitalize.
        output_columns: A list of new names for the columns with the capitalized values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the capitalized values.
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

    def do_apply(self, col):
        return col.str.capitalize()


class StrSplitRule(BaseStrRule):
    ...


class StrSplitRejoinRule(BaseStrRule):
    ...


class StrStripRule(BaseStrRule):
    ...


class StrReplaceRule(BaseStrRule):
    ...


class StrPadRule(BaseStrRule):
    ...


class StrExtractRule(BaseStrRule):
    ...
