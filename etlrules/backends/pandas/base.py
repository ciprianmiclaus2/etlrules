from typing import Optional

from etlrules.backends.pandas.validation import ColumnsInOutMixin
from etlrules.data import RuleData
from etlrules.rule import UnaryOpBaseRule


class BaseAssignColumnRule(UnaryOpBaseRule, ColumnsInOutMixin):

    def __init__(self, input_column: str, output_column:Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert input_column and isinstance(input_column, str), "input_column must be a non-empty string."
        assert output_column is None or (output_column and isinstance(output_column, str)), "output_column must be None or a non-empty string."
        self.input_column = input_column
        self.output_column = output_column

    def do_apply(self, df, col):
        raise NotImplementedError()

    def apply(self, data: RuleData):
        df = self._get_input_df(data)
        input_column, output_column = self.validate_in_out_columns(df, self.input_column, self.output_column, self.strict)
        df = df.assign(**{output_column: self.do_apply(df, df[input_column])})
        self._set_output_df(data, df)
