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
    def __init__(self, sort_by, ascending=True, named_input=None, named_output=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert isinstance(sort_by, str) or (isinstance(sort_by, (list, tuple)) and all(isinstance(val, str) for val in sort_by)), "sort_by must be a str (single column) or a list of str (multiple columns)"
        self.sort_by = sort_by
        if isinstance(self.sort_by, str):
            self.sort_by = [self.sort_by]
        assert isinstance(ascending, bool) or (isinstance(ascending, (list, tuple)) and all(isinstance(val, bool) for val in ascending) and len(ascending) == len(self.sort_by)), "ascending must be a bool or a list of bool of the same len as sort_by"
        self.ascending = ascending

    def apply(self, data):
        df = self._get_input_df(data)
        df = df.sort_values(by=self.sort_by, ascending=self.ascending, ignore_index=True)
        self._set_output_df(data, df)
