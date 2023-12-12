import logging
from numpy import nan
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
)

from .base import DaskMixin, is_pyarrow_string_enabled

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


class StrSplitRule(StrSplitRuleBase, DaskMixin):
    def do_apply(self, df, col):
        perf_logger.warning("StrSplitRule is not vectorized and might hurt the overall performance.")
        new_col = col.str.split(pat=self.separator, n=self.limit)
        pyarrow_string_enabled = is_pyarrow_string_enabled()
        if pyarrow_string_enabled:
            return new_col.apply(lambda val: eval(val) if not isnull(val) else val, meta=(col.name, "object"))
        return new_col.apply(lambda val: val if val not in (nan, NA, None) else val, meta=(col.name, "object"))


class StrSplitRejoinRule(StrSplitRejoinRuleBase, DaskMixin):
    def do_apply(self, df, col):
        perf_logger.warning("StrSplitRejoinRule is not vectorized and might hurt the overall performance.")
        new_col = col.str.split(pat=self.separator, n=self.limit)
        new_separator = self.new_separator
        pyarrow_string_enabled = is_pyarrow_string_enabled()
        if self.sort is not None:
            reverse = self.sort==self.SORT_DESCENDING
            if pyarrow_string_enabled:
                func = lambda val: new_separator.join(sorted(eval(val), reverse=reverse)) if not isnull(val) else val
            else:
                func = lambda val: new_separator.join(sorted(val, reverse=reverse)) if val not in (nan, NA, None) else val
        else:
            if pyarrow_string_enabled:
                func = lambda val: new_separator.join(eval(val)) if not isnull(val) else val
            else:
                func = lambda val: new_separator.join(val) if val not in (nan, NA, None) else val
        return new_col.apply(func, meta=(col.name, "string")).astype({col.name: 'string'})


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
                new_column = new_col[group]
                if group == 0 and self.keep_original_value:
                    # only the first new column keeps the value (in case of multiple groups)
                    new_column = new_column.fillna(value=df[col])
                new_cols_dict[output_columns[idx * groups + group]] = new_column
        df = df.assign(**new_cols_dict)
        self._set_output_df(data, df)
