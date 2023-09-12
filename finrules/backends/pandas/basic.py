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
    def __init__(self, mapper, named_input=None, named_output=None, strict=True):
        assert isinstance(mapper, dict), "mapper needs to be a dict {old_name:new_name}"
        assert all(isinstance(key, str) and isinstance(val, str) for key, val in mapper.items()), "mapper needs to be a dict {old_name:new_name} where the names are str"
        super().__init__(named_input=named_input, named_output=named_output, strict=strict)
        self.mapper = mapper

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict:
            assert set(self.mapper.keys()) <= set(df.columns), f"Missing columns to rename: {set(self.mapper.keys()) - set(df.columns)}"
        df = df.rename(columns=self.mapper)
        self._set_output_df(data, df)
