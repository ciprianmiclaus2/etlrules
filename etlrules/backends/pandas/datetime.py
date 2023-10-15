import datetime
import locale
try:
    from pandas._config.localization import can_set_locale
except:
    can_set_locale = None
from pandas import to_timedelta, isnull, to_datetime
from pandas.tseries.offsets import DateOffset, BusinessDay
from pandas.api.types import is_timedelta64_dtype, is_datetime64_any_dtype
from typing import Optional, Literal, Union

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.backends.pandas.base import BaseAssignColumnRule
from etlrules.backends.pandas.validation import ColumnsInOutMixin
from etlrules.rule import UnaryOpBaseRule


class BaseDateRoundTruncRule(UnaryOpBaseRule, ColumnsInOutMixin):

    UNITS = {
        "day": "D",
        "hour": "H",
        "minute": "T",
        "second": "S",
        "millisecond": "L",
        "microsecond": "U",
        "nanosecond": "N",
    }

    def __init__(self, input_column: str, unit: str, output_column:Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.input_column = input_column
        self.output_column = output_column
        assert isinstance(unit, str) and unit in self.UNITS.keys(), f"unit must be one of {self.UNITS.keys()} and not '{unit}'"
        self.unit = unit

    def do_apply(self, series):
        raise NotImplementedError("Implement in a derived class.")

    def apply(self, data):
        df = self._get_input_df(data)
        input_column, output_column = self.validate_in_out_columns(df, self.input_column, self.output_column, self.strict)
        df = df.assign(**{output_column: self.do_apply(df[input_column])})
        self._set_output_df(data, df)


class DateTimeRoundRule(BaseDateRoundTruncRule):
    """ Rounds a set of datetime columns to the specified granularity (day, hour, minute, etc.).

    Basic usage::

        # rounds the A column to the nearest second
        rule = DateTimeRoundRule("A", "second")
        rule.apply(data)

        # rounds the A column to days
        rule = DateTimeRoundRule("A", "day")
        rule.apply(data)

    Args:
        input_column (str): The column name to round according to the unit specified.
        unit (str): Specifies the unit of rounding.
            That is: rounding to the nearest day, hour, minute, etc.

            The supported units are:
                day: anything up to 12:00:00 rounds down to the current day, after that up to the next day
                hour: anything up to 30th minute rounds down to the current hour, after that up to the next hour
                minute: anything up to 30th second rounds down to the current minute, after that up to the next minute
                second: rounds to the nearest second (if the column has milliseconds)
                millisecond: rounds to the nearest millisecond (if the column has microseconds)
                microsecond: rounds to the nearest microsecond
                nanosecond: rounds to the nearest nanosecond

        output_column (Optional[str]): The name of a new column with the result. Optional.
            If not provided, the result is updated in place.
            In strict mode, if provided, the output_column must not exist in the input dataframe.
            In non-strict mode, if provided, the output_column with overwrite a column with
            the same name in the input dataframe (if any).

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
        MissingColumnError: raised if the input_column column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, overwriting existing columns is ignored.
    """

    def do_apply(self, series):
        return series.dt.round(
            freq=self.UNITS[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundDownRule(BaseDateRoundTruncRule):
    """ Rounds down (truncates) a set of datetime columns to the specified granularity (day, hour, minute, etc.).

    Basic usage::

        # rounds the A column to the nearest second
        rule = DateTimeRoundDownRule("A", "second")
        rule.apply(data)

        # rounds the A column to days
        rule = DateTimeRoundDownRule("A", "day")
        rule.apply(data)

    Args:
        input_column (str): The column name to round according to the unit specified.
        unit (str): Specifies the unit of rounding.
            That is: rounding to the nearest day, hour, minute, etc.

            The supported units are:
                day: removes the hours/minutes/etc.
                hour: removes the minutes/seconds etc.
                minute: removes the seconds/etc.
                second: removes the milliseconds/etc.
                millisecond: removes the microseconds
                microsecond: removes nanoseconds (if any)

        output_column (Optional[str]): The name of a new column with the result. Optional.
            If not provided, the result is updated in place.
            In strict mode, if provided, the output_column must not exist in the input dataframe.
            In non-strict mode, if provided, the output_column with overwrite a column with
            the same name in the input dataframe (if any).

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
        MissingColumnError: raised if the input_column column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, overwriting existing columns is ignored.
    """

    def do_apply(self, series):
        return series.dt.floor(
            freq=self.UNITS[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundUpRule(BaseDateRoundTruncRule):
    """ Rounds up a set of datetime columns to the specified granularity (day, hour, minute, etc.).

    Basic usage::

        # rounds the A column to the nearest second
        rule = DateTimeRoundUpRule("A", "second")
        rule.apply(data)

        # rounds A column to days
        rule = DateTimeRoundUpRule("A", "day")
        rule.apply(data)

    Args:
        input_column (str): The column name to round according to the unit specified.
        unit (str): Specifies the unit of rounding.
            That is: rounding to the nearest day, hour, minute, etc.

            The supported units are:
                day: Rounds up to the next day if there are any hours/minutes/etc.
                hour: Rounds up to the next hour if there are any minutes/etc.
                minute: Rounds up to the next minute if there are any seconds/etc.
                second: Rounds up to the next second if there are any milliseconds/etc.
                millisecond: Rounds up to the next millisecond if there are any microseconds
                microsecond: Rounds up to the next microsecond if there are any nanoseconds

        output_column (Optional[str]): The name of a new column with the result. Optional.
            If not provided, the result is updated in place.
            In strict mode, if provided, the output_column must not exist in the input dataframe.
            In non-strict mode, if provided, the output_column with overwrite a column with
            the same name in the input dataframe (if any).

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
        MissingColumnError: raised if the input_column column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, overwriting existing columns is ignored.
    """

    def do_apply(self, series):
        return series.dt.ceil(
            freq=self.UNITS[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeExtractComponentRule(BaseAssignColumnRule):
    """ Extract an individual component of a date/time (e.g. year, month, day, hour, etc.).

    Basic usage::

        # extracts the year component from col_A. E.g. 2023-05-05 10:00:00 will extract 2023
        rule = DateTimeExtractComponentRule("col_A", component="year")
        rule.apply(data)

    Args:
        input_column (str): A datetime column to extract the given component from.
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
            Use the command `locale -a` on your terminal on Unix systems to find your locale language code.
            Trying to set the locale to a value that doesn't appear under the `locale -a` output will fail
            with ValueError: Unsupported locale.
        output_column (Optional[str]): An optional column name to contain the result.
            If provided, if must have the same length as the columns sequence.
            The existing columns are unchanged, and new columns are created with the component extracted.
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
        MissingColumnError: raised if the input_column column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.
        ValueError: raised if a locale is specified which is not supported or available on the machine running the scripts.

    Note:
        In non-strict mode, overwriting existing columns is ignored.
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
        "day_name": "day_name",
        "month_name": "month_name",
    }

    def __init__(self, input_column: str, component: str, locale: Optional[str], output_column:Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, 
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

    def do_apply(self, df, col):
        res = getattr(col.dt, self._component)
        if self._component in ("day_name", "month_name"):
            try:
                res = res(locale=self._locale)
            except locale.Error:
                raise ValueError(f"Unsupported locale: {self._locale}")
        if self._component in ("day_name", "month_name"):
            res = res.astype('string')
        else:
            res = res.astype('Int64')
        return res


# date arithmetic
DT_ARITHMETIC_UNITS = {
    "years": "years",
    "months": "months",
    "weeks": "weeks",
    "days": "days",
    "weekdays": None,
    "hours": "hours",
    "minutes": "minutes",
    "seconds": "seconds",
    "milliseconds": "milliseconds",
    "microseconds": "microseconds",
    "nanoseconds": "nanoseconds",
}

DT_TIMEDELTA_UNITS = set(["days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"])


class AddSubBaseRule(BaseAssignColumnRule):

    SIGN = 0

    def __init__(self, input_column: str, unit_value: Union[int, float, str], 
                 unit: Optional[Literal["years", "months", "weeks", "weekdays", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"]],
                 output_column:Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        assert self.SIGN in (1, -1)
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.unit_value = unit_value
        if not isinstance(self.unit_value, str):
            assert unit in DT_ARITHMETIC_UNITS.keys(), f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}"
        self.unit = unit

    def do_apply(self, df, col):
        if isinstance(self.unit_value, str):
            # unit_value is a column
            if self.unit_value not in df.columns:
                raise MissingColumnError(f"Column {self.unit_value} in unit_value does not exist in the input dataframe.")
            col2 = df[self.unit_value]
            if is_datetime64_any_dtype(col2):
                if self.SIGN != -1:  # only supported for substracting a datetime from another datetime
                    raise ValueError(f"Cannot add column {self.unit_value} of type datetime to another datetime column.")
                return col - col2
            elif is_timedelta64_dtype(col2):
                pass  # do nothing for timedelta
            else:
                # another type - will be interpreted as an offset/timedelta
                if self.unit not in DT_ARITHMETIC_UNITS.keys():
                    raise ValueError(f"Unsupported unit: '{self.unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}")
                if self.unit in DT_TIMEDELTA_UNITS:
                    col2 = to_timedelta(col2, unit=DT_ARITHMETIC_UNITS[self.unit], errors="coerce")
                else:
                    unit, sign = self.unit, self.SIGN
                    if self.unit == "weekdays":
                        col2 = col2.apply(lambda x: BusinessDay(sign * (0 if isnull(x) else int(x))))
                    else:
                        col2 = col2.apply(lambda x: DateOffset(**{DT_ARITHMETIC_UNITS[unit]: sign * (0 if isnull(x) else int(x))}))
                    if not col2.empty:
                        col += col2
                    return to_datetime(col, errors='coerce')
            return col + self.SIGN * col2
        if self.unit not in DT_ARITHMETIC_UNITS.keys():
            raise ValueError(f"Unsupported unit: '{self.unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}")
        if self.unit == "weekdays":
            return col + BusinessDay(self.SIGN * self.unit_value)
        return col + DateOffset(**{DT_ARITHMETIC_UNITS[self.unit]: self.SIGN * self.unit_value})


class DateTimeAddRule(AddSubBaseRule):
    """ Adds a number of units (days, hours, minutes, etc.) to a datetime column.

    Basic usage::

        # adds 2 days the A column
        rule = DateTimeAddRule("A", 2, "days")
        rule.apply(data)

        # adds 2 hours to the A column
        rule = DateTimeAddRule("A", 2, "hours")
        rule.apply(data)

    Args:
        input_column (str): The name of a datetime column to add to.
        unit_value (Union[int,float,str]): The number of units to add to the datetime column.
            The unit_value can be negative, in which case this rule performs a substract.

            A name of an existing column can be passed into unit_value, in which case, that
            column will be added to the input_column.
            If the column is a timedelta, it will be added as is, if it's a numeric column,
            then it will be interpreted based on the unit parameter (e.g. years/days/hours/etc.).
            In this case, if the column specified in the unit_value doesn't exist,
            MissingColumnError is raised.

        unit (str): Specifies what unit the unit_value is in. Supported values are:
            years, months, weeks, weekdays, days, hours, minutes, seconds, microseconds, nanoseconds.
            weekdays skips weekends (ie Saturdays and Sundays).

        output_column (Optional[str]): The name of a new column with the result. Optional.
            If not provided, the result is updated in place.
            In strict mode, if provided, the output_column must not exist in the input dataframe.
            In non-strict mode, if provided, the output_column with overwrite a column with
            the same name in the input dataframe (if any).

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
        MissingColumnError: raised if the input_column doesn't exist in the input dataframe.
        MissingColumnError: raised if unit_value is a name of a column but it doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.
        ValueError: raised if unit_value is a column which is not a timedelta column and the unit parameter is not specified.

    Note:
        In non-strict mode, missing columns or overwriting existing columns are ignored.
    """

    SIGN = 1


class DateTimeSubstractRule(AddSubBaseRule):
    """ Substracts a number of units (days, hours, minutes, etc.) from a datetime column.

    Basic usage::

        # substracts 2 days the A column
        rule = DateTimeSubstractRule("A", 2, "days")
        rule.apply(data)

        # substracts 2 hours to the A column
        rule = DateTimeSubstractRule("A", 2, "hours")
        rule.apply(data)

    Args:
        input_column (str): The name of a datetime column to add to.
        unit_value (Union[int,float,str]): The number of units to add to the datetime column.
            The unit_value can be negative, in which case this rule performs an addition.

            A name of an existing column can be passed into unit_value, in which case, that
            column will be substracted from the input_column.
            If the column is a timedelta, it will be substracted as is, if it's a numeric column,
            then it will be interpreted based on the unit parameter (e.g. days/hours/etc.).
            In this case, if the column specified in the unit_value doesn't exist,
            MissingColumnError is raised.

        unit (str): Specifies what unit the unit_value is in. Supported values are:
            days, hours, minutes, seconds, microseconds, nanoseconds.

        output_column (Optional[str]): The name of a new column with the result. Optional.
            If not provided, the result is updated in place.
            In strict mode, if provided, the output_column must not exist in the input dataframe.
            In non-strict mode, if provided, the output_column with overwrite a column with
            the same name in the input dataframe (if any).

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
        MissingColumnError: raised if the input_column doesn't exist in the input dataframe.
        MissingColumnError: raised if unit_value is a name of a column but it doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, missing columns or overwriting existing columns are ignored.
    """

    SIGN = -1


class DateTimeDiffRule(AddSubBaseRule):
    """ Calculates the difference between two datetime columns, optionally extracting it in the specified unit.

    Basic usage::

        # calculates the A - B in days
        rule = DateTimeDiffRule("A", "B", unit="days")
        rule.apply(data)

    Args:
        input_column (str): The name of a datetime column.
        input_column2 (str): The name of the second datetime column.
            The result will be input_column - input_column2

        unit (Optional[str]): If specified, it will extract the given component of the difference:
            years, months, days, hours, minutes, seconds, microseconds, nanoseconds.

        output_column (Optional[str]): The name of a new column with the result. Optional.
            If not provided, the result is updated in place.
            In strict mode, if provided, the output_column must not exist in the input dataframe.
            In non-strict mode, if provided, the output_column with overwrite a column with
            the same name in the input dataframe (if any).

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
        MissingColumnError: raised if either input_column or input_column2 don't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        For best results, round the datetime columns using one of the rounding rules before
        calculating the difference. Otherwise, this rule will tend to truncate/round down.
        For example: 2023-05-05 10:00:00 - 2023-05-04 10:00:01 will result in 0 days even though
        the difference is 23:59:59. In cases like this one, it might be preferable to round, in this
        case perhaps round to "day" using DateTimeRoundRule or DateTimeRoundDownRule. This will result
        in a 2023-05-05 00:00:00 - 2023-05-04 00:00:00 which results in 1 day.

    """

    COMPONENTS = {
        "days": "days",
        "hours": "hours",
        "minutes": "minutes",
        "seconds": "seconds",
        "microseconds": "microseconds",
        "nanoseconds": "nanoseconds",
        "total_seconds": None,
    }

    SIGN = -1

    EXCLUDE_FROM_SERIALIZE = ('unit_value', )

    def __init__(self, input_column: str, input_column2: str, 
                 unit: Optional[Literal["days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds", "total_seconds"]],
                 output_column:Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        assert input_column2 and isinstance(input_column2, str), "input_column2 must be a non-empty string representing the name of a column."
        assert unit is None or unit in self.COMPONENTS.keys(), f"unit must be None of one of: {self.COMPONENTS.keys()}"
        super().__init__(input_column=input_column, unit_value=input_column2, unit=unit, output_column=output_column,
                         named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.input_column2 = input_column2

    def do_apply(self, df, col):
        if self.unit_value not in df.columns:
            raise MissingColumnError(f"Column {self.unit_value} in input_column2 does not exist in the input dataframe.")
        res = super().do_apply(df, col)
        if is_timedelta64_dtype(res) and self.unit:
            if self.unit == "total_seconds":
                res = res.dt.total_seconds()
            else:
                res = res.dt.components[self.unit]
            res = res.astype("Int64")
        return res


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


class DateTimeToStrFormatRule(BaseAssignColumnRule):
    """ Formats a datetime column to a string representation according to a specified format.

    Basic usage::

        # displays the dates in column col_A in the %Y-%m-%d format, e.g. 2023-05-19
        rule = DateTimeToStrFormatRule("col_A", format="%Y-%m-%d")
        rule.apply(data)

    Args:
        input_column (str): The datetime column with the values to format to string.
        format: The format used to display the date/time.
            E.g. %Y-%m-%d
            For the directives accepted in the format, have a look at:
            https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
        output_column (Optional[str]): An optional column to hold the formatted results.
            If provided, the existing column is unchanged, and a new column with this new
            is created.
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
        MissingColumnError: raised if the input column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, overwriting existing columns is ignored.
    """

    def __init__(self, input_column: str, format: str, output_column: Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, 
                         name=name, description=description, strict=strict)
        self.format = format

    def do_apply(self, df, col):
        return col.dt.strftime(self.format).astype('string')
