import datetime
import locale
try:
    from pandas._config.localization import can_set_locale
except:
    can_set_locale = None
from pandas import to_timedelta, isnull, to_datetime
from pandas.tseries.offsets import DateOffset, BusinessDay
from pandas.api.types import is_timedelta64_dtype, is_datetime64_any_dtype

from .base import PandasMixin
from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError

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


ROUND_TRUNC_UNITS_MAPPED = {
    "day": "D",
    "hour": "H",
    "minute": "T",
    "second": "S",
    "millisecond": "L",
    "microsecond": "U",
    "nanosecond": "N",
}


class DateTimeRoundRule(PandasMixin, DateTimeRoundRuleBase):
    def do_apply(self, df, series):
        return series.dt.round(
            freq=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundDownRule(PandasMixin, DateTimeRoundDownRuleBase):
    def do_apply(self, df, series):
        return series.dt.floor(
            freq=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundUpRule(PandasMixin, DateTimeRoundUpRuleBase):
    def do_apply(self, df, series):
        return series.dt.ceil(
            freq=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeExtractComponentRule(PandasMixin, DateTimeExtractComponentRuleBase):

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

    def _cannot_set_locale(self, locale):
        return can_set_locale and not can_set_locale(locale)

    def do_apply(self, df, col):
        component = self.COMPONENTS[self.component]
        res = getattr(col.dt, component)
        if component in ("day_name", "month_name"):
            try:
                res = res(locale=self._locale)
            except locale.Error:
                raise ValueError(f"Unsupported locale: {self._locale}")
        if component in ("day_name", "month_name"):
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




def add_sub_col(df, col, unit_value, unit, sign):
    if isinstance(unit_value, str):
        # unit_value is a column
        if unit_value not in df.columns:
            raise MissingColumnError(f"Column {unit_value} in unit_value does not exist in the input dataframe.")
        col2 = df[unit_value]
        if is_datetime64_any_dtype(col2):
            if sign != -1:  # only supported for substracting a datetime from another datetime
                raise ValueError(f"Cannot add column {unit_value} of type datetime to another datetime column.")
            return col - col2
        elif is_timedelta64_dtype(col2):
            pass  # do nothing for timedelta
        else:
            # another type - will be interpreted as an offset/timedelta
            if unit not in DT_ARITHMETIC_UNITS.keys():
                raise ValueError(f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}")
            if unit in DT_TIMEDELTA_UNITS:
                col2 = to_timedelta(col2, unit=DT_ARITHMETIC_UNITS[unit], errors="coerce")
            else:
                if unit == "weekdays":
                    col2 = col2.apply(lambda x: BusinessDay(sign * (0 if isnull(x) else int(x))))
                else:
                    col2 = col2.apply(lambda x: DateOffset(**{DT_ARITHMETIC_UNITS[unit]: sign * (0 if isnull(x) else int(x))}))
                if not col2.empty:
                    col += col2
                return to_datetime(col, errors='coerce')
        if sign == -1:
            return col - col2
        return col + col2
    if unit not in DT_ARITHMETIC_UNITS.keys():
        raise ValueError(f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}")
    if unit == "weekdays":
        return col + BusinessDay(sign * unit_value)
    return col + DateOffset(**{DT_ARITHMETIC_UNITS[unit]: sign * unit_value})


class DateTimeAddRule(PandasMixin, DateTimeAddRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, 1)


class DateTimeSubstractRule(PandasMixin, DateTimeSubstractRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, -1)


class DateTimeDiffRule(PandasMixin, DateTimeDiffRuleBase):

    COMPONENTS = {
        "days": "days",
        "hours": "hours",
        "minutes": "minutes",
        "seconds": "seconds",
        "microseconds": "microseconds",
        "nanoseconds": "nanoseconds",
        "total_seconds": None,
    }

    def do_apply(self, df, col):
        if self.input_column2 not in df.columns:
            raise MissingColumnError(f"Column {self.input_column2} in input_column2 does not exist in the input dataframe.")
        res = add_sub_col(df, col, self.input_column2, self.unit, -1)
        if is_timedelta64_dtype(res) and self.unit:
            if self.unit == "total_seconds":
                res = res.dt.total_seconds()
            else:
                res = res.dt.components[self.COMPONENTS[self.unit]]
            res = res.astype("Int64")
        return res


class DateTimeUTCNowRule(DateTimeUTCNowRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.columns:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        df = df.assign(**{self.output_column: datetime.datetime.utcnow()})
        self._set_output_df(data, df)


class DateTimeLocalNowRule(DateTimeLocalNowRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.columns:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        df = df.assign(**{self.output_column: datetime.datetime.now()})
        self._set_output_df(data, df)


class DateTimeToStrFormatRule(PandasMixin, DateTimeToStrFormatRuleBase):

    def do_apply(self, df, col):
        return col.dt.strftime(self.format).astype('string')
