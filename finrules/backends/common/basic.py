from finrules.rule import BaseRule


class BaseStartRule(BaseRule):
    def __init__(self, main_input, named_inputs=None):
        self.assert_is_dataframe(main_input, f"main_input needs to be a DataFrame not {type(main_input)}")
        self.main_input = main_input
        if named_inputs:
            assert isinstance(named_inputs, dict), f"named_inputs needs to be a dict name:dataframe and not {type(named_inputs)}"
            for name, df in named_inputs.items():
                assert isinstance(name, str), "Name '{name}' is not valid for a named input, it needs to be a str."
                self.assert_is_dataframe(v, f"named_input {name} needs to be a DataFrame not {type(df)}")
            self.named_inputs = {k: v for k, v in named_inputs}
        else:
            self.named_inputs = {}

    def apply(self, data):
        data.set_main_output(self.main_input)
        for name, named_input_df in self.named_inputs.items():
            data.set_named_output(name, named_input_df)
