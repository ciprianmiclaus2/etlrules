from finrules.data import RuleData
from finrules.rule import BaseRule
from finrules.exceptions import MissingColumn


class BaseProjectRule(BaseRule):
    def __init__(self, columns, exclude=False, named_input=None, named_output=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        assert all(
            isinstance(col, str) for col in self.columns
        ), "ProjectRule: columns must be strings"
        self.exclude = exclude

    def _get_remaining_columns(self, df_column_names):
        columns_set = set(self.columns)
        df_column_names_set = set(df_column_names)
        if self.strict:
            if not columns_set <= df_column_names_set:
                raise MissingColumn(f"No such columns: {columns_set - df_column_names_set}. Available columns: {df_column_names_set}.")
        if self.exclude:
            remaining_columns = [
                col for col in df_column_names if col not in columns_set
            ]
        else:
            remaining_columns = [
                col for col in self.columns if col in df_column_names_set
            ]
        return remaining_columns


class RulesBlock(BaseRule):
    def __init__(self, rules, named_input=None, named_output=None, name=None, description=None, strict=True):
        self._rules = [rule for rule in rules]
        assert self._rules, "RulesBlock: Empty rules set provided."
        assert all(isinstance(rule, BaseRule) for rule in self._rules), [rule for rule in self._rules if not isinstance(rule, BaseRule)]
        assert self._rules[0].named_input is None, "First rule in a RulesBlock must consume the main input/output"
        assert self._rules[-1].named_input is None, "Last rule in a RulesBlock must produce the main output"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)

    def apply(self, data):
        data2 = RuleData(
            main_input=self._get_input_df(data),
            named_inputs={k: v for k, v in data.get_named_outputs()},
            strict=self.strict
        )
        for rule in self._rules:
            rule.apply(data2)
        self._set_output_df(data, data2.get_main_output())

    def to_dict(self):
        dct = super().to_dict()
        dct[self.__class__.__name__]["rules"] = [rule.to_dict() for rule in self._rules]
        return dct

    @classmethod
    def from_dict(cls, dct, backend):
        dct = dct["RulesBlock"]
        rules = [BaseRule.from_dict(rule, backend) for rule in dct.get("rules", ())]
        kwargs = {k: v for k, v in dct.items() if k != "rules"}
        return cls(rules=rules, **kwargs)
