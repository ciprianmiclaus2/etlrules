from typing import Iterable, Optional, Literal

from etlrules.backends.pandas.validation import ColumnsInOutMixin
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
        columns (Iterable[str]): A list of string columns to convert to lower case.
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the lower case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the lower case values.
            If not provided, the result is updated in place.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

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
        columns (Iterable[str]): A list of string columns to convert to upper case.
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

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
        columns (Iterable[str]): A list of string columns to convert to capitalize.
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the capitalized values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the capitalized values.
            If not provided, the result is updated in place.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

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
    """ Strips leading, trailing or both whitespaces or other characters from given columns.

    Basic usage::

        rule = StrStripRule(["col_A", "col_B", "col_C"], how="both")
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to convert to upper case.
        how: How should the stripping be done. One of left, right, both.
            Left strips leading characters, right trailing characters and both at both ends.
        characters: If set, it contains a list of characters to be stripped.
            When not specified or when set to None, whitespace is removed.
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.

    Note:
        In non-strict mode, missing columns are ignored.
    """

    STRIP_LEFT = 'left'
    STRIP_RIGHT = 'right'
    STRIP_BOTH = 'both'

    def __init__(self, columns: Iterable[str], how: Literal[STRIP_LEFT, STRIP_RIGHT, STRIP_BOTH]=STRIP_BOTH, characters: Optional[str]=None, output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(columns=columns, output_columns=output_columns, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert how in (self.STRIP_BOTH, self.STRIP_LEFT, self.STRIP_RIGHT), f"Unknown how parameter {how}. It must be one of: {(self.STRIP_BOTH, self.STRIP_LEFT, self.STRIP_RIGHT)}"
        self.how = how
        self.characters = characters or None

    def do_apply(self, col):
        if self.how == self.STRIP_BOTH:
            return col.str.strip(to_strip=self.characters)
        elif self.how == self.STRIP_RIGHT:
            return col.str.rstrip(to_strip=self.characters)
        return col.str.lstrip(to_strip=self.characters)


class StrReplaceRule(BaseStrRule):
    ...


class StrPadRule(BaseStrRule):
    ...


class StrExtractRule(BaseStrRule):
    ...
