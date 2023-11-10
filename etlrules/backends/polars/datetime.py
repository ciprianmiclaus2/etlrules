import datetime
import locale
import polars as pl
try:
    import polars_business as plb
except:
    plb = None

from .base import PolarsMixin
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
    "day": "1d",
    "hour": "1h",
    "minute": "1m",
    "second": "1s",
    "millisecond": "1ms",
    "microsecond": "1us",
}


class DateTimeRoundRule(PolarsMixin, DateTimeRoundRuleBase):
    def do_apply(self, df, series):
        offset = "-1ns" if self.unit == "microsecond" else "-1us"
        return series.dt.offset_by(offset).dt.round(
            every=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='infer'
        )


class DateTimeRoundDownRule(PolarsMixin, DateTimeRoundDownRuleBase):
    def do_apply(self, df, series):
        return series.dt.truncate(
            every=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='earliest'
        )


class DateTimeRoundUpRule(PolarsMixin, DateTimeRoundUpRuleBase):

    def do_apply(self, df, series):
        return series.dt.offset_by(ROUND_TRUNC_UNITS_MAPPED[self.unit]).dt.offset_by("-1us").dt.truncate(
            every=ROUND_TRUNC_UNITS_MAPPED[self.unit],
            ambiguous='earliest'
        )


class DateTimeExtractComponentRule(PolarsMixin, DateTimeExtractComponentRuleBase):

    COMPONENTS = {
        "year": "year",
        "month": "month",
        "day": "day",
        "hour": "hour",
        "minute": "minute",
        "second": "second",
        "microsecond": "microsecond",
        "weekday": "weekday",
        "day_name": "day_name",
        "month_name": "month_name",
    }

    def _cannot_set_locale(self, loc):
        return loc and not self._set_locale(loc)

    def _set_locale(self, loc):
        if loc:
            loc = loc.split(".")
            if len(loc) != 2:
                return False
            current_locale = None
            try:
                current_locale = locale.getlocale(locale.LC_TIME)
                locale.setlocale(locale.LC_TIME, loc)
                locale.setlocale(locale.LC_TIME, current_locale)
            except locale.Error:
                return False
        return True

    def do_apply(self, df, col):
        component = self.COMPONENTS[self.component]
        if component in ("day_name", "month_name"):
            current_locale = None
            formatting = "%B" if component == "month_name" else "%A"
            try:
                if self._locale:
                    current_locale = locale.getlocale(locale.LC_TIME)
                    locale.setlocale(locale.LC_TIME, self._locale.split("."))
                res = col.dt.strftime(formatting)
            except locale.Error:
                raise ValueError(f"Unsupported locale: {self._locale}")
            finally:
                if current_locale:
                    locale.setlocale(locale.LC_TIME, current_locale)
        else:
            res = getattr(col.dt, component)().cast(pl.Int64)
            if component == "weekday":
                res = res - 1
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
}

OFFSETS = {
    "years": "y",
    "months": "mo",
    "weeks": "w",
    "days": "d",
    "weekdays": "bd",
    "hours": "h",
    "minutes": "m",
    "seconds": "s",
    "milliseconds": "ms",
    "microseconds": "us",
}

DT_TIMEDELTA_UNITS = set(["weeks", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds"])


def add_sub_col(df, col, unit_value, unit, sign, input_column):
    if isinstance(unit_value, str):
        # unit_value is a column
        if unit_value not in df.columns:
            raise MissingColumnError(f"Column {unit_value} in unit_value does not exist in the input dataframe.")
        col2 = df[unit_value]
        if col2.dtype == pl.Datetime:
            if sign != -1:  # only supported for substracting a datetime from another datetime
                raise ValueError(f"Cannot add column {unit_value} of type datetime to another datetime column.")
            return col - col2
        elif col2.dtype == pl.Duration:
            pass  # do nothing for timedelta
        else:
            # another type - will be interpreted as an offset/timedelta
            if unit not in DT_ARITHMETIC_UNITS.keys():
                raise ValueError(f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}")
            if unit in DT_TIMEDELTA_UNITS:
                col2 = pl.duration(**{DT_ARITHMETIC_UNITS[unit]: col2})
            else:
                if unit == "weekdays":
                    if plb is None:
                        raise RuntimeError("Calculation requires polars_business. pip install polars_business.")
                    return plb.col(input_column).bdt.offset_by(
                        col2.map_elements(lambda x: f"{sign*x}bd" if x else x, return_dtype=pl.Utf8), weekend=('Sat', 'Sun'), roll="forward" if sign == -1 else "backward"
                    )
                offset_unit = OFFSETS[unit]
                return col.dt.offset_by(
                    col2.map_elements(lambda x: f"{sign*x}{offset_unit}" if x else x, return_dtype=pl.Utf8)
                )
        if sign == -1:
            return col - col2
        return col + col2
    if unit not in DT_ARITHMETIC_UNITS.keys():
        raise ValueError(f"Unsupported unit: '{unit}'. It must be one of {DT_ARITHMETIC_UNITS.keys()}")
    if unit == "weekdays":
        if plb is None:
            raise RuntimeError("Calculation requires polars_business. pip install polars_business.")
        return plb.col(input_column).bdt.offset_by(f"{sign * unit_value}{OFFSETS[unit]}", weekend=('Sat', 'Sun'), roll="forward" if sign == -1 else "backward")
    return col.dt.offset_by(f"{sign * unit_value}{OFFSETS[unit]}")


class DateTimeAddRule(PolarsMixin, DateTimeAddRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, 1, self.input_column)


class DateTimeSubstractRule(PolarsMixin, DateTimeSubstractRuleBase):

    def do_apply(self, df, col):
        return add_sub_col(df, col, self.unit_value, self.unit, -1, self.input_column)


class DateTimeDiffRule(PolarsMixin, DateTimeDiffRuleBase):

    COMPONENTS = {
        "days": ("days", None),
        "hours": ("hours", 24),
        "minutes": ("minutes", 60),
        "seconds": ("seconds", 60),
        "microseconds": ("microseconds", 1000),
        "total_seconds": ("seconds", None),
    }

    def do_apply(self, df, col):
        if self.input_column2 not in df.columns:
            raise MissingColumnError(f"Column {self.input_column2} in input_column2 does not exist in the input dataframe.")
        res = add_sub_col(df, col, self.input_column2, self.unit, -1, self.input_column)
        if res.dtype == pl.Duration and self.unit:
            method, mod = self.COMPONENTS[self.unit]
            res = getattr(res.dt, method)()
            if mod is not None:
                res = res % mod
        return res


class DateTimeUTCNowRule(DateTimeUTCNowRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.columns:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        df = df.with_columns(
            pl.lit(datetime.datetime.utcnow()).alias(self.output_column)
        )
        self._set_output_df(data, df)


class DateTimeLocalNowRule(DateTimeLocalNowRuleBase):

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.columns:
            raise ColumnAlreadyExistsError(f"{self.output_column} already exists in the input dataframe.")
        df = df.with_columns(
            pl.lit(datetime.datetime.now()).alias(self.output_column)
        )
        self._set_output_df(data, df)


class DateTimeToStrFormatRule(PolarsMixin, DateTimeToStrFormatRuleBase):

    def do_apply(self, df, col):
        return col.dt.strftime(self.format)
