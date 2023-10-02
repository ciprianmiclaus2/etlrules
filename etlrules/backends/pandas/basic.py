from typing import Literal, Iterable, Mapping, Optional, Union

from etlrules.backends.common.basic import BaseProjectRule
from etlrules.exceptions import MissingColumnError
from etlrules.rule import UnaryOpBaseRule

from .validation import PandasRuleValidationMixin


class ProjectRule(BaseProjectRule, PandasRuleValidationMixin):
    """ Reshapes the data frame to keep, eliminate or re-order the set of columns.

    Args:
        columns (Iterable[str]): The list of columns to keep or eliminate from the data frame.
            The order of column names will be reflected in the result data frame, so this rule can be used to re-order columns.
        exclude (bool): When set to True, the columns in the columns arg will be excluded from the data frame. Boolean. Default: False
            In strict mode, if any column specified in the columns arg doesn't exist in the input data frame, a MissingColumnError exception is raised.
            In non strict mode, the missing columns are ignored.

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
        MissingColumnError: raised in strict mode only, if any columns are missing from the input data frame.
    """

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        remaining_columns = self._get_remaining_columns(df.columns)
        df = df[remaining_columns]
        self._set_output_df(data, df)


class RenameRule(UnaryOpBaseRule):
    """ Renames a set of columns in the data frame.

    Args:
        mapper: A dictionary of old names (keys) and new names (values) to be used for the rename operation
            The order of column names will be reflected in the result data frame, so this rule can be used to re-order columns.

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
        MissingColumnError: raised in strict mode only, if any columns (keys) are missing from the input data frame.
    """

    def __init__(self, mapper: Mapping[str, str], named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        assert isinstance(mapper, dict), "mapper needs to be a dict {old_name:new_name}"
        assert all(isinstance(key, str) and isinstance(val, str) for key, val in mapper.items()), "mapper needs to be a dict {old_name:new_name} where the names are str"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.mapper = mapper

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        if self.strict:
            if not set(self.mapper.keys()) <= set(df.columns):
                raise MissingColumnError(f"Missing columns to rename: {set(self.mapper.keys()) - set(df.columns)}")
        df = df.rename(columns=self.mapper)
        self._set_output_df(data, df)


class SortRule(UnaryOpBaseRule):
    """ Sort the input dataframe by the given columns, either ascending or descending.

    Args:
        sort_by: Either a single column speified as a string or a list or tuple of columns to sort by
        ascending: Whether to sort ascending or descending. Boolean. Default: True

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

    Note:
        When multiple columns are specified, the first column decides the sort order.
        For any rows that have the same value in the first column, the second column is used to decide the sort order within that group and so on.
    """

    def __init__(self, sort_by: Iterable[str], ascending: Union[bool,Iterable[bool]]=True, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.sort_by = [col for col in sort_by]
        if isinstance(self.sort_by, str):
            self.sort_by = [self.sort_by]
        assert isinstance(ascending, bool) or (isinstance(ascending, (list, tuple)) and all(isinstance(val, bool) for val in ascending) and len(ascending) == len(self.sort_by)), "ascending must be a bool or a list of bool of the same len as sort_by"
        self.ascending = ascending

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df = df.sort_values(by=self.sort_by, ascending=self.ascending, ignore_index=True)
        self._set_output_df(data, df)


class DedupeRule(UnaryOpBaseRule):
    """ De-duplicates by dropping duplicates using a set of columns to determine the duplicates.

    It has logic to keep the first, last or none of the duplicate in a set of duplicates.

    Args:
        columns: A subset of columns in the data frame which are used to determine the set of duplicates.
            Any rows that have the same values in these columns are considered to be duplicates.
        keep: What to keep in the de-duplication process. One of:
            first: keeps the first row in the duplicate set
            last: keeps the last row in the duplicate set
            none: drops all the duplicates

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
        MissingColumnError: raised when a column specified to deduplicate on doesn't exist in the input data frame.

    Note:
        MissingColumnError is raised in both strict and non-strict modes. This is because the rule cannot operate reliably without a correct set of columns.
    """

    KEEP_FIRST = 'first'
    KEEP_LAST = 'last'
    KEEP_NONE = 'none'

    ALL_KEEPS = (KEEP_FIRST, KEEP_LAST, KEEP_NONE)
 
    def __init__(self, columns: Iterable[str], keep: Literal[KEEP_FIRST, KEEP_LAST, KEEP_NONE]=KEEP_FIRST, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        assert all(
            isinstance(col, str) for col in self.columns
        ), "DedupeRule: columns must be strings"
        assert keep in self.ALL_KEEPS, f"DedupeRule: keep must be one of: {self.ALL_KEEPS}"
        self.keep = False if keep == DedupeRule.KEEP_NONE else keep

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        if not set(self.columns) <= set(df.columns):
            raise MissingColumnError(f"Missing column(s) to dedupe on: {set(self.columns) - set(df.columns)}")
        df = df.drop_duplicates(subset=self.columns, keep=self.keep, ignore_index=True)
        self._set_output_df(data, df)