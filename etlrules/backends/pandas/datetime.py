import datetime
from typing import Iterable, Optional, Literal

from etlrules.exceptions import ColumnAlreadyExistsError
from etlrules.backends.pandas.base import BaseAssignRule
from etlrules.backends.pandas.validation import ColumnsInOutMixin
from etlrules.rule import UnaryOpBaseRule


class DateTimeRoundRule(BaseAssignRule):
    ...


class DateTimeFloorRule(BaseAssignRule):
    ...


class DateTimeCeilingRule(BaseAssignRule):
    ...


class DateTimeExtractComponentRule(BaseAssignRule):
    """ Extract an individual component of a date/time (e.g. year, month, day, hour, etc.). """


class DateTimeExtractComponentNamesRule(BaseAssignRule):
    """ Extract the weekday name or month name from a date. """


# date arithmetic

class DateTimeAddRule(BaseAssignRule):
    ...


class DateTimeSubstractRule(BaseAssignRule):
    ...


class DateTimeDiffRule(BaseAssignRule):
    ...


class DateTimeUTCNowRule(UnaryOpBaseRule):
    ...


class DateTimeLocalNowRule(UnaryOpBaseRule):
    def __init__(self, output_column, named_input:Optional[str]=None, named_output:Optional[str]=None, name:Optional[str]=None, description:Optional[str]=None, strict:bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert output_column and isinstance(output_column, str)
        self.output_column = output_column

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.columns:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        df = df.assign(**{self.output_column: datetime.datetime.now()})
        self._set_output_df(data, df)


class DateTimeToStrFormatRule(BaseAssignRule):
    ...
