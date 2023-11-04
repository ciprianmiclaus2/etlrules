import re
from typing import Iterable, Optional, Literal

from etlrules.backends.common.base import BaseAssignColumnRule
from etlrules.rule import ColumnsInOutMixin, UnaryOpBaseRule


class StrLowerRule(BaseAssignColumnRule):
    """ Converts the values in a string column to lower case.

    Basic usage::

        rule = StrLowerRule("col_A")
        rule.apply(data)

    Args:
        input_column (str): A string column to convert to lower case.
        output_column (Optional[str]): An optional new names for the column with the lower case values.
            If provided, the existing column is unchanged, and a new column is created with the lower case values.
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
        MissingColumnError: raised if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """


class StrUpperRule(BaseAssignColumnRule):
    """ Converts the values in a string columns to upper case.

    Basic usage::

        rule = StrUpperRule("col_A")
        rule.apply(data)

    Args:
        input_column (str): A string column to convert to upper case.
        output_column (Optional[str]): An optional new names for the column with the upper case values.
            If provided, the existing column is unchanged, and a new column is created with the upper case values.
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
        MissingColumnError: raised if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """


class StrCapitalizeRule(BaseAssignColumnRule):
    """ Converts the values in a string column to capitalized values.

    Capitalization will convert the first letter in the string to upper case and the rest of the letters
    to lower case.

    Basic usage::

        rule = StrCapitalizeRule("col_A")
        rule.apply(data)

    Args:
        input_column (str): A string column with the values to capitalize.
        output_column (Optional[str]): An optional new names for the column with the capitalized values.
            If provided, the existing column is unchanged, and a new column is created with the capitalized values.
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
        MissingColumnError: raised if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """


class StrSplitRule(BaseAssignColumnRule):
    """ Splits a string into an array of substrings based on a string separator.

    Note:
        The output is an array of substrings which can optionally be limited via the limit parameter to only
        include the first <limit> number of substrings.
        If you need the output to be a string, perhaps joined on a different separator and optionally sorted
        then use the StrSplitRejoinRule rule.

    Basic usage::

        # splits the col_A column on ,
        # "a,b;c,d" will be split as ["a", "b;c", "d"]
        rule = StrSplitRule("col_A", separator=",")
        rule.apply(data)

    Args:
        input_column (str): A string column to split.
        separator: A literal value to split the string by.
        limit: A limit to the number of substrings. If specified, only the first <limit> substrings are returned
            plus an additional remainder. At most, limit + 1 substrings are returned with the last beind the remainder.
        output_column (Optional[str]): An optional column to hold the result of the split.
            If provided, the existing column is unchanged and a new column is created with the result.
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
        MissingColumnError: raised if the input_column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """

    def __init__(self, input_column: str, separator: str, limit: Optional[int]=None, output_column: Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert separator and isinstance(separator, str)
        self.separator = separator
        self.limit = limit


class StrSplitRejoinRule(BaseAssignColumnRule):
    """ Splits the values in a string column into an array of substrings based on a string separator then rejoin with a new separator, optionally sorting the substrings.

    Note:
        The output is an array of substrings which can optionally be limited via the limit parameter to only
        include the first <limit> number of substrings.

    Basic usage::

        # splits the col_A column on ,
        # "b,d;a,c" will be split and rejoined as "b|c|d;a"
        rule = StrSplitRejoinRule("col_A", separator=",", new_separator="|", sort="ascending")
        rule.apply(data)

    Args:
        input_column (str): The column to split and rejoin.
        separator: A literal value to split the string by.
        limit: A limit to the number of substrings. If specified, only the first <limit> substrings are returned
            plus an additional remainder. At most, limit + 1 substrings are returned with the last beind the remainder.
        new_separator: A new separator used to rejoin the substrings.
        sort: Optionally sorts the substrings before rejoining using the new_separator.
            It can be set to either ascending or descending, sorting the substrings accordingly.
            When the value is set to None, there is no sorting.
        output_column (Optional[str]): An optional new column to hold the result.
            If provided, the existing column is unchanged and a new column is created with the result.
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
        MissingColumnError: raised if the input_column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """

    SORT_ASCENDING = "ascending"
    SORT_DESCENDING = "descending"

    def __init__(self, input_column: str, separator: str, limit:Optional[int]=None, new_separator:str=",", sort:Optional[Literal[SORT_ASCENDING, SORT_DESCENDING]]=None, output_column:Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert separator and isinstance(separator, str)
        self.separator = separator
        self.limit = limit
        assert isinstance(new_separator, str) and new_separator
        self.new_separator = new_separator
        assert sort in (None, self.SORT_ASCENDING, self.SORT_DESCENDING)
        self.sort = sort


class StrStripRule(BaseAssignColumnRule):
    """ Strips leading, trailing or both whitespaces or other characters from the values in the input column.

    Basic usage::

        rule = StrStripRule("col_A", how="both")
        rule.apply(data)

    Args:
        input_column (str): A input column to strip characters from its values.
        how: How should the stripping be done. One of left, right, both.
            Left strips leading characters, right trailing characters and both at both ends.
        characters: If set, it contains a list of characters to be stripped.
            When not specified or when set to None, whitespace is removed.
        output_column (Optional[str]): An optional new column to hold the results.
            If provided, the existing column is unchanged and a new column is created with the results.
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
        MissingColumnError: raised if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """

    STRIP_LEFT = 'left'
    STRIP_RIGHT = 'right'
    STRIP_BOTH = 'both'

    def __init__(self, input_column: str, how: Literal[STRIP_LEFT, STRIP_RIGHT, STRIP_BOTH]=STRIP_BOTH, characters: Optional[str]=None, output_column: Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert how in (self.STRIP_BOTH, self.STRIP_LEFT, self.STRIP_RIGHT), f"Unknown how parameter {how}. It must be one of: {(self.STRIP_BOTH, self.STRIP_LEFT, self.STRIP_RIGHT)}"
        self.how = how
        self.characters = characters or None


class StrPadRule(BaseAssignColumnRule):
    """ Makes strings of a given width (justifies) by padding left or right with a fill character.

    Basic usage::

        # a value of ABCD will ABCD....
        rule = StrPadRule("col_A", width=8, fill_character=".", how="right")
        rule.apply(data)

    Args:
        input_column (str): A string column to be padded.
        width: Pad with the fill_character to this width.
        fill_character: Character to fill with. Defaults to whitespace.
        how: How should the stripping be done. One of left or right.
            Left pads at the beggining of the string, right pads at the end. Default: left.
        output_column (Optional[str]): An optional new column with the padded results.
            If provided, the existing column is unchanged and a new column is created with the results.
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
        MissingColumnError: raised if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """

    PAD_LEFT = 'left'
    PAD_RIGHT = 'right'

    def __init__(self, input_column: str, width: int, fill_character: str, how: Literal[PAD_LEFT, PAD_RIGHT]=PAD_LEFT, output_column:Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        assert how in (self.PAD_LEFT, self.PAD_RIGHT), f"Unknown how parameter {how}. It must be one of: {(self.PAD_LEFT, self.PAD_RIGHT)}"
        self.how = how
        self.width = width
        self.fill_character = fill_character


class StrExtractRule(UnaryOpBaseRule, ColumnsInOutMixin):
    r""" Extract substrings from strings columns using regular expressions.

    Basic usage::

        # extracts the number between start_ and _end
        # ie: for an input value of start_1234_end - will extract 1234 in col_A
        rule = StrExtractRule("col_A", regular_expression=r"start_([\d]*)_end")
        rule.apply(data)

        # extracts with multiple groups, extracting the single digit at the end as well
        # for an input value of start_1234_end_9, col_1 will extract 1234, col_2 will extract 9
        rule = StrExtractRule("col_A", regular_expression=r"start_([\d]*)_end_([\d])", output_columns=["col_1", "col_2"])
        rule.apply(data)

    Args:
        input_column (str): A column to extract data from.
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
        output_columns (Optional[Iterable[str]]): A list of new names for the result columns.
            Optional. If provided, it must have one output_column per regular expression group.
            For example, given the regular expression "a_([\d])_([\d])" with 2 groups, then
            the output columns must have 2 columns (one per group) - for example ["out_1", "out_2"].
            The existing columns are unchanged, and new columns are created with extracted values.
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
        MissingColumnError: raised if the input_column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if an output_column already exists in the dataframe.
        ValueError: raised if output_columns is provided and not the same length as the number of groups in the regular expression.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """

    def __init__(self, input_column: str, regular_expression: str, keep_original_value: bool=False, output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        self.input_column = input_column
        self.output_columns = [out_col for out_col in output_columns] if output_columns else None
        self.regular_expression = regular_expression
        self._compiled_expr = re.compile(regular_expression)
        groups = self._compiled_expr.groups
        assert groups > 0, "The regular expression must have at least 1 group - ie a section in () - which gets extracted."
        if self.output_columns is not None:
            if len(self.output_columns) != groups:
                raise ValueError(f"The regular expression has {groups} group(s), the output_columns must have {groups} column(s).")
        if groups > 1 and self.output_columns is None:
            raise ValueError(f"The regular expression has more than 1 groups in which case output_columns must be specified (one per group).")
        self.keep_original_value = keep_original_value
