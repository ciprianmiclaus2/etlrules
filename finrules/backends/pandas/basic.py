from finrules.backends.common.basic import BaseProjectRule, BaseStartRule
from finrules.rule import BaseRule

from .validation import PandasRuleValidationMixin


class StartRule(BaseStartRule, PandasRuleValidationMixin):
    ...


class ProjectRule(BaseProjectRule, PandasRuleValidationMixin):
    def apply(self, data):
        df = self._get_input_df(data)
        remaining_columns = self._get_remaining_columns(df.columns)
        df = df[remaining_columns]
        self._set_output_df(data, df)


class RenameRule(BaseRule):
    def __init__(self, mapper, named_input=None, named_output=None, name=None, description=None, strict=True):
        assert isinstance(mapper, dict), "mapper needs to be a dict {old_name:new_name}"
        assert all(isinstance(key, str) and isinstance(val, str) for key, val in mapper.items()), "mapper needs to be a dict {old_name:new_name} where the names are str"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.mapper = mapper

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict:
            assert set(self.mapper.keys()) <= set(df.columns), f"Missing columns to rename: {set(self.mapper.keys()) - set(df.columns)}"
        df = df.rename(columns=self.mapper)
        self._set_output_df(data, df)


class SortRule(BaseRule):
    """ Sort the input dataframe by the given columns, either ascending or descending.

    Params:
        sort_by: Either a single column speified as a string or a list or tuple of columns to sort by
        ascending: Whether to sort ascending or descending. Boolean. Default: True

    Common params:
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
    def __init__(self, sort_by, ascending=True, named_input=None, named_output=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert sort_by and (isinstance(sort_by, str) or (isinstance(sort_by, (list, tuple)) and all(isinstance(val, str) for val in sort_by))), "sort_by must be a str (single column) or a list of str (multiple columns)"
        self.sort_by = sort_by
        if isinstance(self.sort_by, str):
            self.sort_by = [self.sort_by]
        assert isinstance(ascending, bool) or (isinstance(ascending, (list, tuple)) and all(isinstance(val, bool) for val in ascending) and len(ascending) == len(self.sort_by)), "ascending must be a bool or a list of bool of the same len as sort_by"
        self.ascending = ascending

    def apply(self, data):
        df = self._get_input_df(data)
        df = df.sort_values(by=self.sort_by, ascending=self.ascending, ignore_index=True)
        self._set_output_df(data, df)
