from typing import Iterable, Optional

from etlrules.exceptions import MissingColumnError
from etlrules.rule import UnaryOpBaseRule


class BaseStrRule(UnaryOpBaseRule):

    def __init__(self, columns: Iterable[str], named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]

    def do_apply(self, col):
        raise NotImplementedError

    def apply(self, data):
        df = self._get_input_df(data)
        df_cols_set = set(df.columns)
        if self.strict:
            if not set(self.columns) <= df_cols_set:
                raise MissingColumnError(f"Column(s) {set(self.columns) - df_cols_set} are missing from the input dataframe.")
            columns = self.columns
        else:
            columns = [col for col in self.columns if col in df_cols_set]
        df = df.assign(**{col: self.do_apply(df[col]) for col in columns})
        self._set_output_df(data, df)


class StrLowerRule(BaseStrRule):
    def do_apply(self, col):
        return col.str.lower()


class StrUpperRule(BaseStrRule):
    def do_apply(self, col):
        return col.str.upper()


class StrCapitalizeRule(BaseStrRule):
    def do_apply(self, col):
        return col.str.capitalize()


class StrSplitRule(BaseStrRule):
    ...


class StrSplitRejoinRule(BaseStrRule):
    ...


class StrStripRule(BaseStrRule):
    ...


class StrReplaceRule(BaseStrRule):
    ...


class StrPadRule(BaseStrRule):
    ...


class StrExtractRule(BaseStrRule):
    ...
