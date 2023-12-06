import datetime
import locale
try:
    from pandas._config.localization import can_set_locale
except:
    can_set_locale = None
import dask.dataframe as dd
import numpy as np
from pandas import isnull
from pandas.tseries.offsets import DateOffset, BusinessDay
from pandas.api.types import is_timedelta64_dtype, is_datetime64_any_dtype

from .base import DaskMixin
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


class DateTimeRoundRule(DaskMixin, DateTimeRoundRuleBase):
    def do_apply(self, df, series):
        return series.dt.round(
            freq=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundDownRule(DaskMixin, DateTimeRoundDownRuleBase):
    def do_apply(self, df, series):
        return series.dt.floor(
            freq=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeRoundUpRule(DaskMixin, DateTimeRoundUpRuleBase):
    def do_apply(self, df, series):
        return series.dt.ceil(
            freq=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='infer',
            nonexistent='shift_forward'
        )


class DateTimeExtractComponentRule(DaskMixin, DateTimeExtractComponentRuleBase):

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


def business_day_offset(dt_col, offset_col, strict=True):
    # TODO: tidy up this logic
    offset_col = offset_col.fillna(0)
    col = dt_col.dt.weekday.fillna(0)
    # -1 -> 0, 1 -> 3
    offset_sign_adj = ((offset_col // offset_col.replace({0: 1}).abs() + 1) // 2) * 3
    col2_weekend_offset = col.replace({0: np.nan, 1: np.nan, 2: np.nan, 3: np.nan, 4: np.nan, 5: 2, 6: 1}) - offset_sign_adj
    col2_weekend_offset = col2_weekend_offset.fillna(0)
    dt_col2 = dt_col + dd.to_timedelta(col2_weekend_offset, unit="D", errors="raise" if strict else "coerce")
    col = dt_col2.dt.weekday.fillna(0)
    col2 = offset_col + col
    col3 = (
        (col2 // 5) * 7 + (col2 % 5)
    ) - col
    col4 = dd.to_timedelta(col3, unit="D", errors="raise" if strict else "coerce")
    return dt_col2 + col4


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
                col2 = dd.to_timedelta(col2, unit=DT_ARITHMETIC_UNITS[unit], errors="coerce")
            else:
                if unit == "weekdays":
                    return business_day_offset(col, sign * col2)
                elif unit == "weeks":
                    col += dd.to_timedelta(col2.fillna(0) * sign * 7, "D", errors="coerce")
                    return dd.to_datetime(col, errors="coerce")
                # TODO: implement offset in years/months
                #"years": "years",
                #"months": "months",
                col2 = col2.apply(lambda x: DateOffset(**{DT_ARITHMETIC_UNITS[unit]: sign * (0 if isnull(x) else int(x))}), meta=("", "object"))
                #if col2.size.compute():
                col += col2
                return dd.to_datetime(col, errors='coerce')
        if sign == -1:
            return col - col2
        return col + col2
    if unit not in DT_ARITHMETIC_UNITS.keys():
        raise ValueError(f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}")
    if unit == "weekdays":
        return col + BusinessDay(sign * unit_value)
    return col + DateOffset(**{DT_ARITHMETIC_UNITS[unit]: sign * unit_value})


class DateTimeAddRule(DaskMixin, DateTimeAddRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, 1)


class DateTimeSubstractRule(DaskMixin, DateTimeSubstractRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, -1)


class DateTimeDiffRule(DaskMixin, DateTimeDiffRuleBase):

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


class DateTimeToStrFormatRule(DaskMixin, DateTimeToStrFormatRuleBase):

    def do_apply(self, df, col):
        return col.dt.strftime(self.format).astype('string')
