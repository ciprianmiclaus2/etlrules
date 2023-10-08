import datetime
import locale
try:
    from pandas._config.localization import can_set_locale
except:
    can_set_locale = None
from typing import Iterable, Optional, Literal, Sequence, Union

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.backends.pandas.base import BaseAssignRule
from etlrules.backends.pandas.validation import ColumnsInOutMixin
from etlrules.rule import UnaryOpBaseRule


class BaseDateRoundTruncRule(UnaryOpBaseRule, ColumnsInOutMixin):

    GRANULARITIES = {
        "day": "D",
        "hour": "H",
        "minute": "T",
        "second": "S",
        "millisecond": "L",
        "microsecond": "U",
        "nanosecond": "N",
    }

    def __init__(self, columns: Iterable[str], granularity: Union[str, Sequence[str]], output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        self.output_columns = [out_col for out_col in output_columns] if output_columns else None
        self.granularity = granularity
        if isinstance(self.granularity, str):
            self._granularities = [self.granularity] * len(self.columns)
        elif isinstance(self.granularity, (list, tuple)):
            assert len(self.granularity) == len(self.columns), "Granularity must have the same len as the columns when passed in as a sequence."
            self._granularities = self.granularity
        else:
            assert False, "granularity must be a str or a sequence of strings."
        assert all(granularity in self.GRANULARITIES.keys() for granularity in self._granularities)

    def do_apply(self, series, granularity):
        raise NotImplementedError("Implement in a derived class.")

    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(df, self.columns, self.output_columns, self.strict)
        df = df.assign(**{output_col: self.do_apply(df[col], self._granularities[idx]) for idx, (col, output_col) in enumerate(zip(columns, output_columns))})
        self._set_output_df(data, df)


class DateTimeRoundRule(BaseDateRoundTruncRule):
    """ Rounds a set of datetime columns to the specified granularity (day, hour, minute, etc.).

    Basic usage::

        # rounds the A, B and C column to the nearest second
        rule = DateTimeRoundRule(["A", "B", "C"], "second")
        rule.apply(data)

        # rounds A to days, B to hours, C to seconds
        rule = DateTimeRoundRule(["A", "B", "C"], ["day", "hour", "second"])
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to round according to the granularity specification.
        granularity (Union[str, Sequence[str]]): Specifies the granularity of rounding.
            That is: rounding to the nearest day, hour, minute, etc.
            It's either a string value, in which case it is applied to all columns or a
            list or tuple of different granularity, in which case they need to be the same length
            as the columns iterable and they applied positionally to the columns.

            The supported granularities are:
                day: anything up to 12:00:00 rounds down to the current day, after that up to the next day
                hour: anything up to 30th minute rounds down to the current hour, after that up to the next hour
                minute: anything up to 30th second rounds down to the current minute, after that up to the next minute
                second: rounds to the nearest second (if the column has milliseconds)
                millisecond: rounds to the nearest millisecond (if the column has microseconds)
                microsecond: rounds to the nearest microsecond
                nanosecond: rounds to the nearest nanosecond

        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place.

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
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if an output_column already exists in the dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.

    Note:
        In non-strict mode, missing columns or overwriting existing columns are ignored.
    """

    def do_apply(self, series, granularity):
        return series.dt.round(
            freq=self.GRANULARITIES[granularity],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundDownRule(BaseDateRoundTruncRule):
    """ Rounds down (truncates) a set of datetime columns to the specified granularity (day, hour, minute, etc.).

    Basic usage::

        # rounds the A, B and C column to the nearest second
        rule = DateTimeRoundDownRule(["A", "B", "C"], "second")
        rule.apply(data)

        # rounds A to days, B to hours, C to seconds
        rule = DateTimeRoundDownRule(["A", "B", "C"], ["day", "hour", "second"])
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to round according to the granularity specification.
        granularity (Union[str, Sequence[str]]): Specifies the granularity of rounding.
            That is: rounding to the nearest day, hour, minute, etc.
            It's either a string value, in which case it is applied to all columns or a
            list or tuple of different granularity, in which case they need to be the same length
            as the columns iterable and they applied positionally to the columns.

            The supported granularities are:
                day: removes the hours/minutes/etc.
                hour: removes the minutes/seconds etc.
                minute: removes the seconds/etc.
                second: removes the milliseconds/etc.
                millisecond: removes the microseconds
                microsecond: removes nanoseconds (if any)

        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place.

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
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if an output_column already exists in the dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.

    Note:
        In non-strict mode, missing columns or overwriting existing columns are ignored.
    """

    def do_apply(self, series, granularity):
        return series.dt.floor(
            freq=self.GRANULARITIES[granularity],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundUpRule(BaseDateRoundTruncRule):
    """ Rounds up a set of datetime columns to the specified granularity (day, hour, minute, etc.).

    Basic usage::

        # rounds the A, B and C column to the nearest second
        rule = DateTimeRoundUpRule(["A", "B", "C"], "second")
        rule.apply(data)

        # rounds A to days, B to hours, C to seconds
        rule = DateTimeRoundUpRule(["A", "B", "C"], ["day", "hour", "second"])
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to round according to the granularity specification.
        granularity (Union[str, Sequence[str]]): Specifies the granularity of rounding.
            That is: rounding to the nearest day, hour, minute, etc.
            It's either a string value, in which case it is applied to all columns or a
            list or tuple of different granularity, in which case they need to be the same length
            as the columns iterable and they applied positionally to the columns.

            The supported granularities are:
                day: Rounds up to the next day if there are any hours/minutes/etc.
                hour: Rounds up to the next hour if there are any minutes/etc.
                minute: Rounds up to the next minute if there are any seconds/etc.
                second: Rounds up to the next second if there are any milliseconds/etc.
                millisecond: Rounds up to the next millisecond if there are any microseconds
                microsecond: Rounds up to the next microsecond if there are any nanoseconds

        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place.

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
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if an output_column already exists in the dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.

    Note:
        In non-strict mode, missing columns or overwriting existing columns are ignored.
    """

    def do_apply(self, series, granularity):
        return series.dt.ceil(
            freq=self.GRANULARITIES[granularity],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeExtractComponentRule(BaseAssignRule):
    """ Extract an individual component of a date/time (e.g. year, month, day, hour, etc.).

    Basic usage::

        # displays the dates in %Y-%m-%d format, e.g. 2023-05-19
        rule = DateTimeExtractComponentRule(["col_A", "col_B", "col_C"], component="year")
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to convert to upper case.
        component: The component of the datatime to extract from the datetime.
            When the component is one of (year, month, day, hour, minute, second, microsecond) then
            the extracted component will be an integer with the respective component of the datetime.
            
            When component is weekday, the component will be an integer with the values 0-6, with
            Monday being 0 and Sunday 6.

            When the component is weekday_name or month_name, the result column will be a string
            column with the names of the weekdays (e.g. Monday, Tuesday, etc.) or month names
            respectively (e.g. January, February, etc.). The names will be printed in the language
            specified in the locale parameter (or English as the default).

        locale: An optional locale string applicable to weekday_name and month_name. When specified,
            the names will use the given locale to print the names in the given language.
            Default: en_US.utf8 will print the names in English.
            Setting this locale to 
            Use the command `locale -a` on your terminal on Unix systems to find your locale language code.
            Trying to set the locale to a value that doesn't appear under the `locale -a` output will fail
            with ValueError: Unsupported locale.
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place.

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
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if an output_column already exists in the dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.
        ValueError: raised if a locale is specified which is not supported or available on the machine running the scripts.

    Note:
        In non-strict mode, missing columns or overwriting existing columns are ignored.
    """

    COMPONENTS = {
        "year": "year",
        "month": "month",
        "day": "day",
        "hour": "hour",
        "minute": "minute",
        "second": "second",
        "microsecond": "microsecond",
        "nanosecond": "nanosecond",
        "weekday": "weekday",
        "weekday_name": "day_name",
        "month_name": "month_name",
    }

    def __init__(self, columns: Iterable[str], component: str, locale: Optional[str], output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(columns=columns, output_columns=output_columns, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        self.component = component
        assert self.component in self.COMPONENTS, f"Unsupported component={self.component}. Must be one of: {self.COMPONENTS.keys()}"
        self._component = self.COMPONENTS[self.component]
        self.locale = locale
        self._locale = self.locale
        if self.locale and can_set_locale and not can_set_locale(locale):
            if self.strict:
                raise ValueError(f"Unsupported locale: {locale}")
            self._locale = None

    def do_apply(self, col):
        res = getattr(col.dt, self._component)
        if self._component in ("day_name", "month_name"):
            try:
                res = res(locale=self._locale)
            except locale.Error:
                raise ValueError(f"Unsupported locale: {self._locale}")
        return res


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
    """ Formats a datetime column to a string representation according to a specified format.

    Basic usage::

        # displays the dates in %Y-%m-%d format, e.g. 2023-05-19
        rule = DateTimeToStrFormatRule(["col_A", "col_B", "col_C"], format="%Y-%m-%d")
        rule.apply(data)

    Args:
        columns (Iterable[str]): A list of string columns to convert to upper case.
        format: The format used to display the date/time.
            E.g. %Y-%m-%d
            For the directives accepted in the format, have a look at:
            https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
        output_columns (Optional[Iterable[str]]): A list of new names for the columns with the upper case values.
            Optional. If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the upper case values.
            If not provided, the result is updated in place.

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
        MissingColumnError: raised in strict mode only if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if an output_column already exists in the dataframe.
        ValueError: raised if output_columns is provided and not the same length as the columns parameter.

    Note:
        In non-strict mode, missing columns or overwriting existing columns are ignored.
    """

    def __init__(self, columns: Iterable[str], format: str, output_columns:Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(columns=columns, output_columns=output_columns, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        self.format = format

    def do_apply(self, col):
        return col.dt.strftime(self.format)
