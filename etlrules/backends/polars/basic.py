import polars as pl
import re
from typing import Iterable, Optional, Union

from etlrules.exceptions import MissingColumnError
from etlrules.rule import UnaryOpBaseRule

from etlrules.backends.common.basic import (
    DedupeRule as DedupeRuleBase,
    RenameRule as RenameRuleBase,
    ReplaceRule as ReplaceRuleBase,
)


class DedupeRule(DedupeRuleBase):
    def do_dedupe(self, df):
        return df.unique(subset=self.columns, keep=self.keep, maintain_order=True)


class RenameRule(RenameRuleBase):
    def do_rename(self, df, mapper):
        return df.rename(mapper)


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

    Raises:
        MissingColumnError: raised when a column in the sort_by doesn't exist in the input dataframe.

    Note:
        When multiple columns are specified, the first column decides the sort order.
        For any rows that have the same value in the first column, the second column is used to decide the sort order within that group and so on.
    """

    def __init__(self, sort_by: Iterable[str], ascending: Union[bool,Iterable[bool]]=True, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        if isinstance(sort_by, str):
            self.sort_by = [sort_by]
        else:
            self.sort_by = [s for s in sort_by]
        assert isinstance(ascending, bool) or (isinstance(ascending, (list, tuple)) and all(isinstance(val, bool) for val in ascending) and len(ascending) == len(self.sort_by)), "ascending must be a bool or a list of bool of the same len as sort_by"
        self.ascending = ascending

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        if not set(self.sort_by) <= set(df.columns):
            raise MissingColumnError(f"Column(s) {set(self.sort_by) - set(df.columns)} are missing from the input dataframe.")
        df = df.sort_values(by=self.sort_by, ascending=self.ascending, ignore_index=True)
        self._set_output_df(data, df)


class ReplaceRule(ReplaceRuleBase):

    def _get_old_new_regex(self, old_val, new_val):
        compiled = re.compile(old_val)
        groupindex = compiled.groupindex
        if compiled.groups > 0 and not groupindex:
            groupindex = {v: v for v in range(1, compiled.groups + 1)}
        for group_name, group_idx in groupindex.items():
            new_val = new_val.replace(f"${group_name}", f"${{{group_name}}}")
            new_val = new_val.replace(f"\\g<{group_name}>", f"${{{group_name}}}")
            new_val = new_val.replace(f"\\{group_idx}", f"${{{group_idx}}}")
        return old_val, new_val

    def do_replace(self, df, input_column, output_column):
        col = df[input_column]
        if self.regex:
            for old_val, new_val in zip(self.values, self.new_values):
                old_val, new_val = self._get_old_new_regex(old_val, new_val)
                col = col.str.replace(old_val, new_val)
        else:
            col = col.map_dict(dict(zip(self.values, self.new_values)), default=pl.first())
        return df.with_columns(
            col.alias(output_column)
        )
