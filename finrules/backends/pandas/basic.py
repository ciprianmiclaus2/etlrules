from finrules.backends.common.basic import BaseProjectRule, BaseStartRule

from .validation import PandasRuleValidationMixin


class StartRule(BaseStartRule, PandasRuleValidationMixin):
    ...


class ProjectRule(BaseProjectRule, PandasRuleValidationMixin):
    def apply(self, data):
        df = self._get_input_df(data)
        remaining_columns = self._get_remaining_columns(df.columns)
        df = df[remaining_columns]
        data.set_main_output(df)
