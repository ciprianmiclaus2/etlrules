import polars as pl

from .base import PolarsMixin
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


class StrLowerRule(StrLowerRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        return col.str.to_lowercase()


class StrUpperRule(StrUpperRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        return col.str.to_uppercase()


class StrCapitalizeRule(StrCapitalizeRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        return col.str.to_titlecase()


class StrSplitRule(StrSplitRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        if self.limit is not None:
            # self.limit + 1 to mimic pandas
            struct_col = col.str.splitn(by=self.separator, n=self.limit + 1)
            df = pl.DataFrame().with_columns(
                pl.concat_list(struct_col.struct[i] for i in range(self.limit + 1)).list.drop_nulls().alias("fld_0")
            ).with_columns(
                # replace empty lists with null
                pl.when(pl.col("fld_0").list.len() == 0).then(None).otherwise(pl.col("fld_0")).name.keep()
            )
            return df["fld_0"]
        return col.str.split(by=self.separator)


class StrSplitRejoinRule(StrSplitRejoinRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        if self.limit is not None:
            # self.limit + 1 to mimic pandas
            struct_col = col.str.splitn(by=self.separator, n=self.limit + 1)
            df = pl.DataFrame().with_columns(
                pl.concat_list(
                    struct_col.struct[i] for i in range(self.limit + 1)
                ).list.drop_nulls().alias("fld_0")
            ).with_columns(
                # replace empty lists with null
                pl.when(
                    pl.col("fld_0").list.len() == 0
                ).then(None).otherwise(
                    pl.col("fld_0")
                ).name.keep()
            )
            new_col = df["fld_0"]
        else:
            new_col = col.str.split(by=self.separator)
        new_separator = self.new_separator
        if self.sort is not None:
            reverse = self.sort==self.SORT_DESCENDING
            func = lambda val: new_separator.join(sorted(val, reverse=reverse))
        else:
            func = lambda val: new_separator.join(val)
        return new_col.map_elements(func, pl.Utf8)


class StrStripRule(StrStripRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        if self.how == self.STRIP_BOTH:
            return col.str.strip_chars(self.characters)
        elif self.how == self.STRIP_RIGHT:
            return col.str.strip_chars_end(self.characters)
        return col.str.strip_chars_start(self.characters)


class StrPadRule(StrPadRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        if self.how == self.PAD_LEFT:
            return col.str.pad_start(self.width, fill_char=self.fill_character)
        return col.str.pad_end(self.width, fill_char=self.fill_character)


class StrExtractRule(StrExtractRuleBase, PolarsMixin):
    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(df.columns, [self.input_column], self.output_columns, self.strict, validate_length=False)
        groups = self._compiled_expr.groups
        input_column = columns[0]
        ordered_cols = [col for col in df.columns]
        ordered_cols += [col for col in output_columns if col not in ordered_cols]
        if self.keep_original_value:
            res = df.with_columns(
                pl.col(input_column).str.extract_groups(self.regular_expression).alias("_tmp_col")
            ).select(
                *([col for col in df.columns] + [pl.col("_tmp_col").struct[i].alias("_tmp_col2" if i == 0 else output_columns[i]) for i in range(groups)])
            )
            res= res.with_columns(
                pl.when(
                    pl.col("_tmp_col2").is_null()
                ).then(pl.col(input_column)).otherwise(pl.col("_tmp_col2")).alias(output_columns[0])
            )
        else:
            res = df.with_columns(
                pl.col(input_column).str.extract_groups(self.regular_expression).alias("_tmp_col")
            ).select(
                *([col for col in df.columns if col not in output_columns] + [pl.col("_tmp_col").struct[i].alias(output_columns[i]) for i in range(groups)])
            )
        
        res = res[ordered_cols]
        self._set_output_df(data, res)
