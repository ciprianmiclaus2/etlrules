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
    """ Adds a new column with the UTC date/time.

    Basic usage::

        rule = DateTimeUTCNowRule(output_column="UTCTimeNow")
        rule.apply(data)

    Args:
        output_column: The name of the column to be added to the dataframe.
            This column will be populated with the UTC date/time at the time of the call.
            The same value will be populated for all rows.
            The date/time populated is a "naive" datetime ie: doesn't have a timezone information.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the input dataframe.

    Note:
        In non-strict mode, if the output_column exists in the input dataframe, it will be overwritten.
    """

    def __init__(self, output_column, named_input:Optional[str]=None, named_output:Optional[str]=None, name:Optional[str]=None, description:Optional[str]=None, strict:bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert output_column and isinstance(output_column, str)
        self.output_column = output_column

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.columns:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        df = df.assign(**{self.output_column: datetime.datetime.utcnow()})
        self._set_output_df(data, df)


class DateTimeLocalNowRule(UnaryOpBaseRule):
    """ Adds a new column with the local date/time.

    Basic usage::

        rule = DateTimeLocalNowRule(output_column="LocalTimeNow")
        rule.apply(data)

    Args:
        output_column: The name of the column to be added to the dataframe.
            This column will be populated with the local date/time at the time of the call.
            The same value will be populated for all rows.
            The date/time populated is a "naive" datetime ie: doesn't have a timezone information.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the input dataframe.

    Note:
        In non-strict mode, if the output_column exists in the input dataframe, it will be overwritten.
    """

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
