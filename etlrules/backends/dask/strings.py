import logging

import numpy as np
import pandas as pd
from pandas import NA, isnull

from etlrules.backends.common.strings import (
    StrLowerRule as StrLowerRuleBase,
    StrUpperRule as StrUpperRuleBase,
    StrCapitalizeRule as StrCapitalizeRuleBase,
    StrSplitRule as StrSplitRuleBase,
    StrSplitRejoinRule as StrSplitRejoinRuleBase,
    StrStripRule as StrStripRuleBase,
    StrPadRule as StrPadRuleBase,
    StrExtractRule as StrExtractRuleBase,
    StrSliceRule as StrSliceRuleBase,
    StrLengthRule as StrLengthRuleBase,
    StrContainsRule as StrContainsRuleBase,
)

from .base import DaskMixin

perf_logger = logging.getLogger("etlrules.perf")


class StrLowerRule(StrLowerRuleBase, DaskMixin):
    def do_apply(self, df, col):
        return col.str.lower()


class StrUpperRule(StrUpperRuleBase, DaskMixin):
    def do_apply(self, df, col):
        return col.str.upper()


class StrCapitalizeRule(StrCapitalizeRuleBase, DaskMixin):
    def do_apply(self, df, col):
        return col.str.capitalize()


def _split_partition(series, separator, limit):
    # returns python lists (object dtype), whatever the storage of the strings is (python or pyarrow)
    split = series.str.split(pat=separator, n=limit, regex=False)
    return pd.Series(
        [list(val) if not _is_na(val) else val for val in split],
        index=series.index, name=series.name, dtype="object"
    )


def _split_rejoin_partition(series, separator, limit, new_separator, sort):
    split = _split_partition(series, separator, limit)
    if sort is None:
        func = lambda val: new_separator.join(val)
    else:
        func = lambda val: new_separator.join(sorted(val, reverse=sort == StrSplitRejoinRuleBase.SORT_DESCENDING))
    return pd.Series(
        [func(val) if not _is_na(val) else NA for val in split],
        index=series.index, name=series.name, dtype="string"
    )


def _is_na(val):
    return not isinstance(val, (list, tuple, np.ndarray)) and isnull(val)


class StrSplitRule(StrSplitRuleBase, DaskMixin):
    def do_apply(self, df, col):
        perf_logger.warning("StrSplitRule is not vectorized and might hurt the overall performance.")
        return col.map_partitions(
            _split_partition, self.separator, self.limit, meta=(col.name, "object")
        )


class StrSplitRejoinRule(StrSplitRejoinRuleBase, DaskMixin):
    def do_apply(self, df, col):
        perf_logger.warning("StrSplitRejoinRule is not vectorized and might hurt the overall performance.")
        return col.map_partitions(
            _split_rejoin_partition, self.separator, self.limit, self.new_separator, self.sort,
            meta=(col.name, "string")
        )


class StrStripRule(StrStripRuleBase, DaskMixin):
    def do_apply(self, df, col):
        if self.how == self.STRIP_BOTH:
            return col.str.strip(to_strip=self.characters)
        elif self.how == self.STRIP_RIGHT:
            return col.str.rstrip(to_strip=self.characters)
        return col.str.lstrip(to_strip=self.characters)


class StrPadRule(StrPadRuleBase, DaskMixin):
    def do_apply(self, df, col):
        if self.how == self.PAD_LEFT:
            return col.str.rjust(self.width, fillchar=self.fill_character)
        return col.str.ljust(self.width, fillchar=self.fill_character)


class StrExtractRule(StrExtractRuleBase, DaskMixin):
    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(df.columns, [self.input_column], self.output_columns, self.strict, validate_length=False)
        new_cols_dict = {}
        groups = self._compiled_expr.groups
        for idx, col in enumerate(columns):
            new_col = df[col].str.extract(self._compiled_expr, expand=True)
            for group in range(groups):
                # by position: the columns are named after the groups if the regular expression has named groups
                new_column = new_col.iloc[:, group]
                if group == 0 and self.keep_original_value:
                    # only the first new column keeps the value (in case of multiple groups)
                    new_column = new_column.fillna(value=df[col])
                new_cols_dict[output_columns[idx * groups + group]] = new_column
        df = df.assign(**new_cols_dict)
        self._set_output_df(data, df)


class StrSliceRule(StrSliceRuleBase, DaskMixin):

    def do_apply(self, df, col):
        return col.str.slice(self.start, self.end)


class StrLengthRule(StrLengthRuleBase, DaskMixin):

    def do_apply(self, df, col):
        return col.str.len().astype("Int64")


class StrContainsRule(StrContainsRuleBase, DaskMixin):

    def do_apply(self, df, col):
        if self.how == self.HOW_STARTSWITH:
            result = col.str.startswith(self.value)
        elif self.how == self.HOW_ENDSWITH:
            result = col.str.endswith(self.value)
        else:
            result = col.str.contains(self.value, regex=False)
        # NA for missing values (some string types return False)
        return result.astype("boolean").mask(col.isna())
