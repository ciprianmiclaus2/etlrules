from typing import Iterable, Optional

from etlrules.exceptions import MissingColumnError
from etlrules.rule import UnaryOpBaseRule


class BaseFillRule(UnaryOpBaseRule):

    FILL_METHOD = None

    def __init__(self, columns: Iterable[str], sort_by: Optional[Iterable[str]]=None, sort_ascending: bool=True, group_by: Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        assert self.FILL_METHOD is not None
        assert columns, "Columns need to be specified for fill rules."
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        assert all(isinstance(col, str) for col in self.columns), "All columns must be strings in fill rules."
        self.sort_by = sort_by
        if self.sort_by is not None:
            self.sort_by = [col for col in self.sort_by]
            assert all(isinstance(col, str) for col in self.sort_by), "All sort_by columns must be strings in fill rules when specified."
        self.sort_ascending = sort_ascending
        self.group_by = group_by
        if self.group_by is not None:
            self.group_by = [col for col in self.group_by]
            assert all(isinstance(col, str) for col in self.group_by), "All group_by columns must be strings in fill rules when specified."

    def do_apply(self, df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df_columns = [col for col in df.columns]
        if self.sort_by:
            if not set(self.sort_by) <= set(df_columns):
                raise MissingColumnError(f"Missing sort_by column(s) in fill operation: {set(self.sort_by) - set(df_columns)}")
        if self.group_by:
            if not set(self.group_by) <= set(df_columns):
                raise MissingColumnError(f"Missing group_by column(s) in fill operation: {set(self.group_by) - set(df_columns)}")
        df = self.do_apply(df)
        self._set_output_df(data, df)


class ForwardFillRule(BaseFillRule):
    """ Replaces NAs/missing values with the next non-NA value, optionally sorting and grouping the data.

    Example::

        | A   | B  |
        | a   | 1  |
        | b   | NA |
        | a   | NA |

    After a fill forward::

        | A   | B  |
        | a   | 1  |
        | b   | 1  |
        | a   | 1  |  

    After a fill forward with group_by=["A"]::

        | A   | B  |
        | a   | 1  |
        | b   | NA |
        | a   | 1  |
    
    The "a" group has the first non-NA value as 1 and that is used "forward" to fill the 3rd row.
    The "b" group has no non-NA values, so nothing to fill.

    Args:
        columns (Iterable[str]): The list of columns to replaces NAs for.
            The rest of the columns in the dataframe are not affected.
        sort_by (Optional[Iterable[str]]): The list of columns to sort by before the fill operation. Optional.
            Given the previous non-NA values are used, sorting can make a difference in the values uses.
        sort_ascending (bool): When sort_by is specified, True means sort ascending, False sort descending.
        group_by (Optional[Iterable[str]]): The list of columns to group by before the fill operation. Optional.
            The fill values are only used within a group, other adjacent groups are not filled.
            Useful when you want to copy(fill) data at a certain group level.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description Optional[str]: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised if any columns specified in either columns, sort_by or group_by are missing from the dataframe.
    """


class BackFillRule(BaseFillRule):
    """ Replaces NAs/missing values with the next non-NA value, optionally sorting and grouping the data.

    Example::

        | A   | B  |
        | a   | NA |
        | b   | 2  |
        | a   | NA |

    After a fill forward::

        | A   | B  |
        | a   | 2  |
        | b   | 2  |
        | a   | NA |  

    After a fill forward with group_by=["A"]::

        | A   | B  |
        | a   | NA |
        | b   | 2  |
        | a   | NA |
    
    The "a" group has no non-NA value, so it is not filled.
    The "b" group has a non-NA value of 2 but not other NA values, so nothing to fill.

    Args:
        columns (Iterable[str]): The list of columns to replaces NAs for.
            The rest of the columns in the dataframe are not affected.
        sort_by (Optional[Iterable[str]]): The list of columns to sort by before the fill operation. Optional.
            Given the previous non-NA values are used, sorting can make a difference in the values uses.
        sort_ascending (bool): When sort_by is specified, True means sort ascending, False sort descending.
        group_by (Optional[Iterable[str]]): The list of columns to group by before the fill operation. Optional.
            The fill values are only used within a group, other adjacent groups are not filled.
            Useful when you want to copy(fill) data at a certain group level.

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
        MissingColumnError: raised if any columns specified in either columns, sort_by or group_by are missing from the dataframe.
    """
