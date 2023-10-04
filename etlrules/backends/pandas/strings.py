import re
from pandas import NA
from numpy import nan
from typing import Iterable, Optional, Literal, Union, Sequence

from etlrules.backends.pandas.base import BaseAssignRule
from etlrules.backends.pandas.validation import ColumnsInOutMixin
from etlrules.rule import UnaryOpBaseRule


class StrLowerRule(BaseAssignRule):
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


class StrUpperRule(BaseAssignRule):
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


class StrCapitalizeRule(BaseAssignRule):
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


class StrSplitRule(BaseAssignRule):
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


class StrSplitRejoinRule(BaseAssignRule):
    """ Splits a string into an array of substrings based on a string separator or a regular expression, then rejoin with a new separator, optionally sorting the substrings.

    Note:
        The output is an array of substrings which can optionally be limited via the limit parameter to only
        include the first <limit> number of substrings.

    Basic usage::

        # splits col_A, col_B, col_C on ,
        # "b,d;a,c" will be split and rejoined as "b|c|d;a"
        rule = StrSplitRejoinRule(["col_A", "col_B", "col_C"], separator=",", new_separator="|", sort="ascending")
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to convert to upper case.
        separator: A literal value to split the string by. Optional.
        separator_regex: A regular expression to split the string by. Optional
            Note: One and only one of separator or separator_regex must be specified.
        limit: A limit to the number of substrings. If specified, only the first <limit> substrings are returned
            plus an additional remainder. At most, limit + 1 substrings are returned with the last beind the remainder.
        new_separator: A new separator used to rejoin the substrings.
        sort: Optionally sorts the substrings before rejoining using the new_separator.
            It can be set to either ascending or descending, sorting the substrings accordingly.
            When the value is set to None, there is no sorting.
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

    SORT_ASCENDING = "ascending"
    SORT_DESCENDING = "descending"

    def __init__(self, columns: Iterable[str], separator: Optional[str]=None, separator_regex:Optional[str]=None, limit:Optional[int]=None, new_separator:str=",", sort:Optional[Literal[SORT_ASCENDING, SORT_DESCENDING]]=None, output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(columns=columns, output_columns=output_columns, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert bool(separator) != bool(separator_regex), "One and only one of separator and separator_regex can be specified."
        self.separator = separator
        self.separator_regex = separator_regex
        self._compiled_regex = re.compile(self.separator_regex) if self.separator_regex is not None else None
        self.limit = limit
        assert isinstance(new_separator, str) and new_separator
        self.new_separator = new_separator
        assert sort in (None, self.SORT_ASCENDING, self.SORT_DESCENDING)
        self.sort = sort

    def do_apply(self, col):
        if self.separator_regex is not None:
            new_col = col.str.split(pat=self._compiled_regex, n=self.limit, regex=True)
        else:
            new_col = col.str.split(pat=self.separator, n=self.limit, regex=False)
        new_separator = self.new_separator
        if self.sort is not None:
            reverse = self.sort==self.SORT_DESCENDING
            func = lambda val: new_separator.join(sorted(val, reverse=reverse)) if val not in (nan, NA, None) else val
        else:
            func = lambda val: new_separator.join(val) if val not in (nan, NA, None) else val
        return new_col.apply(func)


class StrStripRule(BaseAssignRule):
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


class StrPadRule(BaseAssignRule):
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


class StrExtractRule(UnaryOpBaseRule, ColumnsInOutMixin):
    r""" Extract substrings from strings columns using regular expressions.

    Basic usage::

        # extracts the number between start_ and _end
        # ie: for an input value of start_1234_end - will extract 1234 in col_A
        rule = StrExtractRule(["col_A"], regular_expression=r"start_([\d]*)_end")
        rule.apply(data)

        # extracts with multiple groups, extracting the single digit at the end as well
        # for an input value of start_1234_end_9, col_1 will extract 1234, col_2 will extract 9
        rule = StrExtractRule(["col_A"], regular_expression=r"start_([\d]*)_end_([\d])", output_columns=["col_1", "col_2"])
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to convert to upper case.
        regular_expression: The regular expression used to extract data.
            The regular expression must have 1 or more groups - ie sections between brackets.
            The groups do the actual extraction of data.
            If there is a single group, then the column can be modified in place (ie no output_columns are needed) but
            if there are multiple groups, then output_columns must be specified as each group will be extracted in a new
            output column.
        keep_original_value: Only used in case there isn't a match and it specifies if NA should be used in the output or the original value.
            Defaults: True.
            If the regular expression has multiple groups and therefore multiple output_columns, only the first output column
            will keep the original value, the rest will be populated with NA.
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, it must have one output_column per regular expression group, for every input columns.
            For example, if input column is ["A"] and the regular expression is "a_([\d])_([\d])" with 2 groups, then
            the output columns must have 2 columns (one per group) - for example ["A1_out", "A2_out"].
            If the input columns are ["A", "B"] and the regular expression is "a_([\d])_([\d])" with 2 groups, then
            the output columns must have 2 columns (one per group) for every input column - e.g. ["A1_out", "A2_out", "B1_out", "B2_out"].
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place (only possible if the regular expression has a single group).

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

    Note:
        In non-strict mode, missing columns are ignored.
    """

    def __init__(self, columns: Iterable[str], regular_expression: str, keep_original_value: bool=False, output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        self.output_columns = [out_col for out_col in output_columns] if output_columns else None
        self.regular_expression = regular_expression
        self._compiled_expr = re.compile(regular_expression)
        groups = self._compiled_expr.groups
        assert groups > 0, "The regular expression must have at least 1 group - ie a secstion in () - which gets extracted."
        if groups == 1 and self.output_columns is not None:
            assert len(self.output_columns) == len(self.columns), "The regular expression has one group and the output_columns must match 1 to 1 the length of the columns"
        if groups > 1:
            assert self.output_columns is not None, f"The regular expression has {groups} groups in which case the output_columns must be specified."
            assert len(self.output_columns) == groups * len(self.columns), f"The regular expression has {groups} groups, the output_columns must have {groups} columns per each input column."
        self.keep_original_value = keep_original_value

    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(df, self.columns, self.output_columns, self.strict, validate_length=False)
        new_cols_dict = {}
        groups = self._compiled_expr.groups
        for idx, col in enumerate(columns):
            new_col = df[col].str.extract(self._compiled_expr, expand=True)
            if self.keep_original_value:
                # only the first new column keeps the value (in case of multiple groups)
                new_col[0].fillna(value=df[col], inplace=True)
            for group in range(groups):
                new_cols_dict[output_columns[idx * groups + group]] = new_col[group]
        df = df.assign(**new_cols_dict)
        self._set_output_df(data, df)
