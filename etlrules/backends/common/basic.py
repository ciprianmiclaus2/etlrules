from typing import Iterable, Optional

from etlrules.data import RuleData
from etlrules.rule import BaseRule, UnaryOpBaseRule
from etlrules.exceptions import MissingColumnError


class BaseProjectRule(UnaryOpBaseRule):
    def __init__(self, columns: Iterable[str], exclude=False, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
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
                raise MissingColumnError(f"No such columns: {columns_set - df_column_names_set}. Available columns: {df_column_names_set}.")
        if self.exclude:
            remaining_columns = [
                col for col in df_column_names if col not in columns_set
            ]
        else:
            remaining_columns = [
                col for col in self.columns if col in df_column_names_set
            ]
        return remaining_columns


class RulesBlock(UnaryOpBaseRule):
    """ Groups rules into encapsulated blocks or units of rules that achieve one thing.
    Blocks are reusable and encapsulated to reduce complexity.

    Args:
        rules: An iterable of rules which are part of this block.
            The first rule in the block will take its input from the named_input of the RulesBlock (if any, if not from the main output of the previous rule).
            The last rule in the block will publish the output as the named_output of the RulesBlock (if any, or the main output of the block).
            Any named outputs in the block are not exposed to the rules outside of the block (proper encapsulation).

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
    """

    def __init__(self, rules: Iterable[BaseRule], named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        self._rules = [rule for rule in rules]
        assert self._rules, "RulesBlock: Empty rules set provided."
        assert all(isinstance(rule, BaseRule) for rule in self._rules), [rule for rule in self._rules if not isinstance(rule, BaseRule)]
        assert self._rules[0].named_input is None, "First rule in a RulesBlock must consume the main input/output"
        assert self._rules[-1].named_input is None, "Last rule in a RulesBlock must produce the main output"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)

    def apply(self, data):
        super().apply(data)
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
