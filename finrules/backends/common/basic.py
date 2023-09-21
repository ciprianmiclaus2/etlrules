from finrules.data import RuleData
from finrules.rule import BaseRule, UnaryOpBaseRule
from finrules.exceptions import MissingColumn


class BaseProjectRule(UnaryOpBaseRule):
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


class RulesBlock(UnaryOpBaseRule):
    """ Groups rules into encapsulated blocks or units of rules that achieve one thing.
    Blocks are reusable and encapsulated to reduce complexity.

    Params:
        rules: An iterable of rules which are part of this block.
            The first rule in the block will take its input from the named_input of the RulesBlock (if any, if not from the main output of the previous rule).
            The last rule in the block will publish the output as the named_output of the RulesBlock (if any, or the main output of the block).
            Any named outputs in the block are not exposed to the rules outside of the block (proper encapsulation).

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
    """

    def __init__(self, rules, named_input=None, named_output=None, name=None, description=None, strict=True):
        self._rules = [rule for rule in rules]
        assert self._rules, "RulesBlock: Empty rules set provided."
        assert all(isinstance(rule, BaseRule) for rule in self._rules), [rule for rule in self._rules if not isinstance(rule, BaseRule)]
        assert self._rules[0].named_input is None, "First rule in a RulesBlock must consume the main input/output"
        assert self._rules[-1].named_input is None, "Last rule in a RulesBlock must produce the main output"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)

    def apply(self, data):
        """ Applies the rule to the input data.

        Params:
            data: An instance of RuleData which stores inputs and outputs, including the main outputs and any named inputs/outputs.

        Returns:
            None

        Note:
            apply doesn't return any data but it sets the results on the input data parameter (either main output or a named output depending on the rule set up).
        """
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
