import datetime

import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.datetime import (
    DateTimeLocalNowRule as DateTimeLocalNowRuleBase,
    DateTimeUTCNowRule as DateTimeUTCNowRuleBase,
    DateTimeToStrFormatRule as DateTimeToStrFormatRuleBase,
    DateTimeRoundRule as DateTimeRoundRuleBase,
    DateTimeRoundDownRule as DateTimeRoundDownRuleBase,
    DateTimeRoundUpRule as DateTimeRoundUpRuleBase,
    DateTimeExtractComponentRule as DateTimeExtractComponentRuleBase,
    DateTimeAddRule as DateTimeAddRuleBase,
    DateTimeSubstractRule as DateTimeSubstractRuleBase,
    DateTimeDiffRule as DateTimeDiffRuleBase,
)
from etlrules.backends.pyarrow.base import PyArrowMixin, set_column, to_column
from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError


UNIT_MICROSECONDS = {
    "day": 24 * 3600 * 1_000_000,
    "hour": 3600 * 1_000_000,
    "minute": 60 * 1_000_000,
    "second": 1_000_000,
    "millisecond": 1000,
    "microsecond": 1,
}


def round_half_to_even(series, unit):
    """ Rounds to the nearest unit; ties go to the even unit since the epoch (like pandas). """
    if unit not in UNIT_MICROSECONDS or pa.types.is_timestamp(series.type) and series.type.unit == "ns" and unit == "nanosecond":
        return series
    floor = pc.floor_temporal(series, unit=unit)
    ceil = pc.ceil_temporal(series, unit=unit)
    below = pc.subtract(series, floor)
    above = pc.subtract(ceil, series)
    floor_units = _floor_divide(pc.cast(pc.cast(floor, pa.timestamp("us")), pa.int64()), UNIT_MICROSECONDS[unit])
    floor_is_even = pc.equal(pc.subtract(floor_units, pc.multiply(_floor_divide(floor_units, 2), 2)), 0)
    use_floor = pc.or_(pc.less(below, above), pc.and_(pc.equal(below, above), floor_is_even))
    return pc.if_else(use_floor, floor, ceil)


class DateTimeRoundRule(PyArrowMixin, DateTimeRoundRuleBase):

    def do_apply(self, df, series):
        return round_half_to_even(series, self.unit)


class DateTimeRoundDownRule(PyArrowMixin, DateTimeRoundDownRuleBase):

    def do_apply(self, df, series):
        return pc.floor_temporal(series, unit=self.unit)


class DateTimeRoundUpRule(PyArrowMixin, DateTimeRoundUpRuleBase):

    def do_apply(self, df, series):
        return pc.ceil_temporal(series, unit=self.unit)


class DateTimeExtractComponentRule(PyArrowMixin, DateTimeExtractComponentRuleBase):

    COMPONENTS = {
        "year": pc.year,
        "month": pc.month,
        "day": pc.day,
        "hour": pc.hour,
        "minute": pc.minute,
        "second": pc.second,
        "microsecond": lambda col: pc.add(pc.multiply(pc.millisecond(col), 1000), pc.microsecond(col)),
        "nanosecond": pc.nanosecond,
        "weekday": pc.day_of_week,  # Monday=0
    }

    NAME_FORMATS = {
        "day_name": "%A",
        "month_name": "%B",
    }

    def _cannot_set_locale(self, locale):
        try:
            pc.strftime(pa.array([datetime.datetime(2000, 1, 1)]), format="%A", locale=locale)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError):
            return True
        return False

    def do_apply(self, df, col):
        if self.component in self.NAME_FORMATS:
            try:
                return pc.strftime(col, format=self.NAME_FORMATS[self.component], locale=self._locale or "C")
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise ValueError(f"Unsupported locale: {self._locale}") from exc
        return pc.cast(self.COMPONENTS[self.component](col), pa.int64())


DT_TIMEDELTA_UNITS = {
    # unit: (duration unit, multiplier)
    "weeks": ("s", 7 * 24 * 3600),
    "days": ("s", 24 * 3600),
    "hours": ("s", 3600),
    "minutes": ("s", 60),
    "seconds": ("s", 1),
    "milliseconds": ("ms", 1),
    "microseconds": ("us", 1),
    "nanoseconds": ("ns", 1),
}
DT_ARITHMETIC_UNITS = set(DT_TIMEDELTA_UNITS) | {"years", "months", "weekdays"}


def _floor_divide(a, b):
    """ Integer division rounded towards negative infinity (like python's //). """
    return pc.cast(pc.floor(pc.divide(pc.cast(a, pa.float64()), b)), pa.int64())


def _is_scalar(value):
    return isinstance(value, (int, float))


def _to_duration(value, unit):
    """ Converts a number (scalar or column) of units (e.g. days) to a duration. """
    duration_unit, multiplier = DT_TIMEDELTA_UNITS[unit]
    if _is_scalar(value):
        if duration_unit == "s":
            return pa.scalar(datetime.timedelta(seconds=value * multiplier))
        return pa.scalar(int(value), pa.duration(duration_unit))
    value = pc.cast(value, pa.float64()) if pa.types.is_floating(value.type) else value
    if duration_unit == "s":
        # in microseconds to keep fractions of seconds
        value = pc.multiply(value, multiplier * 1_000_000)
        duration_unit = "us"
    return pc.cast(pc.cast(value, pa.int64()), pa.duration(duration_unit))


FOLL_MONDAY_ADJ_WEEKEND_OFFSETS = [0, 0, 0, 0, 0, 2, 1]
PREV_FRIDAY_ADJ_WEEKEND_OFFSETS = [0, 0, 0, 0, 0, -1, -2]


def _days(days):
    return pc.cast(pc.multiply(pc.cast(days, pa.int64()), 24 * 3600 * 1_000_000), pa.duration("us"))


def _add_days(dt_col, days):
    result = pc.add(dt_col, _days(days))
    if result.type != dt_col.type:
        result = pc.cast(result, dt_col.type)
    return result


def dt_adjust_weekends(dt_col, offset, strict=True):
    """ Moves the weekend days to the following Monday (negative offsets) or the previous Friday. """
    weekdays = pc.day_of_week(dt_col)
    if _is_scalar(offset):
        adjustments = FOLL_MONDAY_ADJ_WEEKEND_OFFSETS if offset < 0 else PREV_FRIDAY_ADJ_WEEKEND_OFFSETS
        weekend_offset = pc.take(pa.array(adjustments, type=pa.int64()), weekdays)
    else:
        offset = pc.fill_null(offset, 0)
        # 3 for positive offsets (previous Friday), 0 for negative offsets or 0 (following Monday)
        divisor = pc.abs(pc.if_else(pc.equal(offset, 0), 1, offset))
        direction = _floor_divide(pc.add(_floor_divide(offset, divisor), 1), 2)
        weekend_offset = pc.subtract(
            pc.take(pa.array(FOLL_MONDAY_ADJ_WEEKEND_OFFSETS, type=pa.int64()), weekdays),
            pc.multiply(direction, 3),
        )
        is_weekend = pc.greater_equal(weekdays, 5)
        weekend_offset = pc.if_else(is_weekend, weekend_offset, 0)
    return _add_days(dt_col, pc.fill_null(weekend_offset, 0))


def business_day_offset(dt_col, offset, strict=True):
    """ Adds offset business days (Monday to Friday) to the datetime column dt_col. """
    dt_col = to_column(dt_col, len(dt_col))
    if _is_scalar(offset):
        offset = offset or 0
    else:
        offset = pc.cast(pc.fill_null(offset, 0), pa.int64())
    dt_col2 = dt_adjust_weekends(dt_col, offset, strict=strict)
    weekdays = pc.fill_null(pc.day_of_week(dt_col2), 0)
    target = pc.add(weekdays, offset)
    # target // 5 * 7 + target % 5 - weekdays
    weeks = _floor_divide(target, 5)
    remainder = pc.subtract(target, pc.multiply(weeks, 5))
    days = pc.subtract(pc.add(pc.multiply(weeks, 7), remainder), weekdays)
    return _add_days(dt_col2, days)


def _is_leap_year(year):
    def divisible(n):
        return pc.equal(pc.subtract(year, pc.multiply(_floor_divide(year, n), n)), 0)
    return pc.and_(divisible(4), pc.or_(pc.invert(divisible(100)), divisible(400)))


MONTH_DAYS = pa.array([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], type=pa.int64())


def _make_dates(year, month, day, unit):
    """ Builds dates (midnight timestamps) from the year, month and day columns. """
    date_str = pc.binary_join_element_wise(
        pc.cast(year, pa.string()),
        pc.utf8_lpad(pc.cast(month, pa.string()), 2, "0"),
        pc.utf8_lpad(pc.cast(day, pa.string()), 2, "0"),
        "-",
    )
    return pc.strptime(date_str, format="%Y-%m-%d", unit=unit)


def months_offset(dt_col, offset, strict=True):
    """ Adds offset months to the datetime column, keeping the time of the day.

    The day is capped to the number of days in the resulting month (e.g. Jan 31 + 1 month = Feb 28/29).
    """
    dt_col = to_column(dt_col, len(dt_col))
    if _is_scalar(offset):
        offset = offset or 0
    else:
        offset = pc.cast(pc.fill_null(offset, 0), pa.int64())
    total_months = pc.add(pc.add(pc.multiply(pc.year(dt_col), 12), pc.subtract(pc.month(dt_col), 1)), offset)
    year = _floor_divide(total_months, 12)
    month = pc.add(pc.subtract(total_months, pc.multiply(year, 12)), 1)
    month_days = pc.take(MONTH_DAYS, month)
    month_days = pc.if_else(pc.and_(_is_leap_year(year), pc.equal(month, 2)), 29, month_days)
    day = pc.min_element_wise(pc.cast(pc.day(dt_col), pa.int64()), month_days)
    time_of_day = pc.subtract(dt_col, pc.floor_temporal(dt_col, unit="day"))
    dates = _make_dates(year, month, day, dt_col.type.unit)
    return pc.cast(pc.add(dates, time_of_day), dt_col.type)


def years_offset(dt_col, offset, strict=True):
    if _is_scalar(offset):
        offset = (offset or 0) * 12
    else:
        offset = pc.multiply(pc.cast(pc.fill_null(offset, 0), pa.int64()), 12)
    return months_offset(dt_col, offset, strict=strict)


def add_sub_col(df, col, unit_value, unit, sign, strict=True):
    if isinstance(unit_value, str):
        # unit_value is a column name
        if unit_value not in df.column_names:
            raise MissingColumnError(f"Column {unit_value} in unit_value does not exist in the input dataframe.")
        col2 = df[unit_value]
        if pa.types.is_timestamp(col2.type):
            if sign != -1:
                raise ValueError(f"Cannot add column {unit_value} of type datetime to another datetime column.")
            return pc.subtract(col, col2)
        elif pa.types.is_duration(col2.type):
            pass
        else:
            if unit not in DT_ARITHMETIC_UNITS:
                raise ValueError(f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS}")
            if unit in DT_TIMEDELTA_UNITS:
                col2 = _to_duration(col2, unit)
            else:
                offset = pc.multiply(pc.cast(col2, pa.int64()), sign)
                if unit == "weekdays":
                    return business_day_offset(col, offset, strict=strict)
                elif unit == "months":
                    return months_offset(col, offset, strict=strict)
                return years_offset(col, offset, strict=strict)
        result = pc.subtract(col, col2) if sign == -1 else pc.add(col, col2)
        return pc.cast(result, col.type) if pa.types.is_timestamp(result.type) and result.type != col.type else result
    if unit not in DT_ARITHMETIC_UNITS:
        raise ValueError(f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS}")
    if unit == "weekdays":
        return business_day_offset(col, sign * unit_value, strict=strict)
    elif unit == "months":
        return months_offset(col, sign * unit_value, strict=strict)
    elif unit == "years":
        return years_offset(col, sign * unit_value, strict=strict)
    result = pc.add(col, _to_duration(sign * unit_value, unit))
    return pc.cast(result, col.type) if result.type != col.type else result


class DateTimeAddRule(PyArrowMixin, DateTimeAddRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, 1, strict=self.strict)


class DateTimeSubstractRule(PyArrowMixin, DateTimeSubstractRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, -1, strict=self.strict)


US_PER = {
    "days": 24 * 3600 * 1_000_000,
    "hours": 3600 * 1_000_000,
    "minutes": 60 * 1_000_000,
    "seconds": 1_000_000,
}


class DateTimeDiffRule(PyArrowMixin, DateTimeDiffRuleBase):

    def do_apply(self, df, col):
        if self.input_column2 not in df.column_names:
            raise MissingColumnError(f"Column {self.input_column2} in input_column2 does not exist in the input dataframe.")
        res = add_sub_col(df, col, self.input_column2, self.unit, -1, strict=self.strict)
        if pa.types.is_duration(res.type) and self.unit:
            # the components of the duration, like pandas' Timedelta.components
            # (days can be negative, the other components are always positive)
            micros = pc.cast(pc.cast(res, pa.duration("us")), pa.int64())
            if self.unit == "total_seconds":
                return pc.cast(_floor_divide(micros, 1_000_000), pa.int64())
            days = _floor_divide(micros, US_PER["days"])
            rest = pc.subtract(micros, pc.multiply(days, US_PER["days"]))
            if self.unit == "days":
                return days
            hours = _floor_divide(rest, US_PER["hours"])
            if self.unit == "hours":
                return hours
            rest = pc.subtract(rest, pc.multiply(hours, US_PER["hours"]))
            minutes = _floor_divide(rest, US_PER["minutes"])
            if self.unit == "minutes":
                return minutes
            rest = pc.subtract(rest, pc.multiply(minutes, US_PER["minutes"]))
            seconds = _floor_divide(rest, US_PER["seconds"])
            if self.unit == "seconds":
                return seconds
            rest = pc.subtract(rest, pc.multiply(seconds, US_PER["seconds"]))
            if self.unit == "microseconds":
                return rest
            # nanoseconds
            nanos = pc.cast(pc.cast(res, pa.duration("ns")), pa.int64())
            return pc.subtract(nanos, pc.multiply(_floor_divide(nanos, 1000), 1000))
        return res


class DateTimeUTCNowRule(DateTimeUTCNowRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.column_names:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        df = set_column(df, self.output_column, pa.array([now] * df.num_rows, type=pa.timestamp("us")))
        self._set_output_df(data, df)


class DateTimeLocalNowRule(DateTimeLocalNowRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.column_names:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        now = datetime.datetime.now()
        df = set_column(df, self.output_column, pa.array([now] * df.num_rows, type=pa.timestamp("us")))
        self._set_output_df(data, df)


class DateTimeToStrFormatRule(PyArrowMixin, DateTimeToStrFormatRuleBase):

    def do_apply(self, df, col):
        if "%f" not in self.format and pa.types.is_timestamp(col.type) and col.type.unit != "s":
            # arrow's %S includes the fractions of a second for sub-second timestamps (python's doesn't)
            col = pc.cast(pc.floor_temporal(col, unit="second"), pa.timestamp("s", tz=col.type.tz))
        return pc.strftime(col, format=self.format)
