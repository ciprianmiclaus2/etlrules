from .data import RuleData


class BaseRule:
    def __init__(self, named_input=None, strict=True):
        self.named_input = named_input
        self.strict = strict

    def _get_input_df(self, data):
        if self.named_input is None:
            return data.get_main_output()
        return data.get_named_output(self.named_input)

    def assert_is_dataframe(self, df, context):
        ...

    def apply(self, data):
        assert isinstance(data, RuleData)
