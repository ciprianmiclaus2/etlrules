from .data import RuleData


class BaseRule:
    def __init__(self, named_input=None, named_output=None, strict=True):
        assert named_input is None or isinstance(named_input, str) and named_input
        assert named_output is None or isinstance(named_output, str) and named_output
        self.named_input = named_input
        self.named_output = named_output
        self.strict = strict

    def _get_input_df(self, data):
        if self.named_input is None:
            return data.get_main_output()
        return data.get_named_output(self.named_input)

    def _set_output_df(self, data, df):
        if self.named_output is None:
            data.set_main_output(df)
        else:
            data.set_named_output(self.named_output, df)

    def assert_is_dataframe(self, df, context):
        ...

    def apply(self, data):
        assert isinstance(data, RuleData)
