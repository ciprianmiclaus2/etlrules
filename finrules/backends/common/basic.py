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
