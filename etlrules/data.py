class RuleData:
    def __init__(self, main_input=None, named_inputs=None, strict=True):
        self.strict = strict
        self.main_output = main_input
        self.named_outputs = (
            {name: df for name, df in named_inputs.items()} if named_inputs else {}
        )
        self.lineage_info = {}

    def get_main_output(self):
        return self.main_output

    def set_main_output(self, df):
        self.main_output = df

    def get_named_output(self, name: str):
        assert name in self.named_outputs, f"No such named output {name}"
        return self.named_outputs[name]

    def set_named_output(self, name, df):
        if self.strict:
            assert (
                name not in self.named_outputs
            ), f"{name} already exists as a named output. It will be overwritten."
        self.named_outputs[name] = df

    def get_named_outputs(self):
        yield from self.named_outputs.items()
