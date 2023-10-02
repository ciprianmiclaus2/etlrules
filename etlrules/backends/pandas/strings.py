import re
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
    """ Splits a string into an array of substrings based on a string separator or a regular expression.

    Note:
        The output is an array of substrings which can optionally be limited via the limit parameter to only
        include the first <limit> number of substrings.
        If you need the output to be a string, perhaps joined on a different separator and optionally sorted
        then use the StrSplitRejoinRule rule.

    Basic usage::

        # splits col_A, col_B, col_C on ,
        # "a,b;c,d" will be split as ["a", "b;c", "d"]
        rule = StrSplitRule(["col_A", "col_B", "col_C"], separator=",")
        rule.apply(data)

        # splits col_A, col_B, col_C on either , or ;
        # "a,b;c,d" will be split as ["a", "b", "c", "d"]
        rule = StrSplitRule(["col_A", "col_B", "col_C"], separator_regex=",|;")
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to convert to upper case.
        separator: A literal value to split the string by. Optional.
        separator_regex: A regular expression to split the string by. Optional
            Note: One and only one of separator or separator_regex must be specified.
        limit: A limit to the number of substrings. If specified, only the first <limit> substrings are returned
            plus an additional remainder. At most, limit + 1 substrings are returned with the last beind the remainder.
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

    def __init__(self, columns: Iterable[str], separator: Optional[str]=None, separator_regex:Optional[str]=None, limit:Optional[int]=None, output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(columns=columns, output_columns=output_columns, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert bool(separator) != bool(separator_regex), "One and only one of separator and separator_regex can be specified."
        self.separator = separator
        self.separator_regex = separator_regex
        self._compiled_regex = re.compile(self.separator_regex) if self.separator_regex is not None else None
        self.limit = limit

    def do_apply(self, col):
        if self.separator_regex is not None:
            return col.str.split(pat=self._compiled_regex, n=self.limit, regex=True)
        return col.str.split(pat=self.separator, n=self.limit, regex=False)


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
    """ Makes strings of a given width (justifies) by padding left, right or both sides with a fill character.

    Basic usage::

        rule = StrPadRule(["col_A", "col_B", "col_C"], width=8, fill_character=".", how="right")
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to convert to upper case.
        width: Pad with the fill_character to this width.
        fill_character: Character to fill with. Defaults to whitespace.
        how: How should the stripping be done. One of left, right, both.
            Left pads at the beggining of the string, right pads at the end, while both pads at both ends.
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

    PAD_LEFT = 'left'
    PAD_RIGHT = 'right'
    PAD_BOTH = 'both'

    def __init__(self, columns: Iterable[str], width: int, fill_character: str, how: Literal[PAD_LEFT, PAD_RIGHT, PAD_BOTH]=PAD_BOTH, output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(columns=columns, output_columns=output_columns, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert how in (self.PAD_LEFT, self.PAD_RIGHT, self.PAD_BOTH), f"Unknown how parameter {how}. It must be one of: {(self.PAD_LEFT, self.PAD_RIGHT, self.PAD_BOTH)}"
        self.how = how
        self.width = width
        self.fill_character = fill_character

    def do_apply(self, col):
        if self.how == self.PAD_RIGHT:
            return col.str.ljust(self.width, fillchar=self.fill_character)
        elif self.how == self.PAD_LEFT:
            return col.str.rjust(self.width, fillchar=self.fill_character)
        return col.str.center(self.width, fillchar=self.fill_character)


class StrExtractRule(BaseStrRule):
    ...
