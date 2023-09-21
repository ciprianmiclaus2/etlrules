from finrules.exceptions import MissingColumn
from finrules.rule import UnaryOpBaseRule


class BaseFillRule(UnaryOpBaseRule):

    FILL_METHOD = None

    def __init__(self, columns, sort_by=None, sort_ascending=True, group_by=None, named_input=None, named_output=None, name=None, description=None, strict=True):
        assert self.FILL_METHOD in ('ffill', 'bfill')
        assert columns, "Columns need to be specified for fill rules."
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        assert all(isinstance(col, str) for col in self.columns), "All columns must be strings in fill rules."
        self.sort_by = sort_by
        if self.sort_by is not None:
            self.sort_by = [col for col in self.sort_by]
            assert all(isinstance(col, str) for col in self.sort_by), "All sort_by columns must be strings in fill rules when specified."
        self.sort_ascending = sort_ascending
        self.group_by = group_by
        if self.group_by is not None:
            self.group_by = [col for col in self.group_by]
            assert all(isinstance(col, str) for col in self.group_by), "All group_by columns must be strings in fill rules when specified."

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df_columns = [col for col in df.columns]
        if self.sort_by:
            if not set(self.sort_by) <= set(df_columns):
                raise MissingColumn(f"Missing sort_by column(s) in fill operation: {set(self.sort_by) - set(df_columns)}")
            df = df.sort_values(by=self.sort_by, ascending=self.sort_ascending, ignore_index=True)
        if self.group_by:
            if not set(self.group_by) <= set(df_columns):
                raise MissingColumn(f"Missing group_by column(s) in fill operation: {set(self.group_by) - set(df_columns)}")
            res = df.groupby(self.group_by)
            res = getattr(res, self.FILL_METHOD)()
            res = res[self.columns]
        else:
            res = df[self.columns]
            res = getattr(res, self.FILL_METHOD)()
        df = df.assign(**{col: res[col] for col in self.columns})
        df = df[df_columns]
        self._set_output_df(data, df)


class ForwardFillRule(BaseFillRule):
    FILL_METHOD = "ffill"


class BackFillRule(BaseFillRule):
    FILL_METHOD = "bfill"
