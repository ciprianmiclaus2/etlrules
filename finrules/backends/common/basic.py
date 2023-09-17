from finrules.rule import BaseRule
from finrules.exceptions import MissingColumn


class BaseStartRule(BaseRule):
    def __init__(self, main_input, named_inputs=None, name=None, description=None, strict=True):
        super().__init__(name=name, description=description, strict=strict)
        self.assert_is_dataframe(
            main_input, f"main_input needs to be a DataFrame not {type(main_input)}"
        )
        self.main_input = main_input
        if named_inputs:
            assert isinstance(
                named_inputs, dict
            ), f"named_inputs needs to be a dict name:dataframe and not {type(named_inputs)}"
            for name, df in named_inputs.items():
                assert isinstance(
                    name, str
                ), "Name '{name}' is not valid for a named input, it needs to be a str."
                self.assert_is_dataframe(
                    df, f"named_input {name} needs to be a DataFrame not {type(df)}"
                )
            self.named_inputs = {k: v for k, v in named_inputs.items()}
        else:
            self.named_inputs = {}

    def apply(self, data):
        data.set_main_output(self.main_input)
        for name, named_input_df in self.named_inputs.items():
            data.set_named_output(name, named_input_df)


class BaseProjectRule(BaseRule):
    def __init__(self, column_names, exclude=False, named_input=None, named_output=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in column_names]
        assert all(
            isinstance(col, str) for col in self.columns
        ), "ProjectRule: column_names must be strings"
        self.named_input = named_input
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
