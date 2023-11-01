import polars as pl

from etlrules.backends.common.concat import VConcatRule as VConcatRuleBase, HConcatRule as HConcatRuleBase


class VConcatRule(VConcatRuleBase):

    def do_concat(self, left_df, right_df):
        if not self.strict:
            left_df_cols = set(left_df.columns)
            right_df_cols = set(right_df.columns)
            if left_df_cols != right_df_cols:
                missing_left = [col for col in right_df.columns if col not in left_df_cols]
                if missing_left:
                    left_df = left_df.with_columns_seq(
                        *[right_df[col][:0].extend_constant(None, len(left_df)) for col in missing_left]
                    )
                missing_right = [col for col in left_df.columns if col not in right_df_cols]
                if missing_right:
                    right_df = right_df.with_columns_seq(
                        *[left_df[col][:0].extend_constant(None, len(right_df)) for col in missing_right]
                    )
                return left_df.vstack(right_df[left_df.columns], in_place=False)

        return left_df.vstack(right_df, in_place=False)


class HConcatRule(HConcatRuleBase):

    def do_concat(self, left_df, right_df):
        left_df_len = len(left_df)
        right_df_len = len(right_df)
        if left_df_len != right_df_len:
            if left_df_len > right_df_len:
                diff = left_df_len - right_df_len
                right_df = pl.DataFrame().with_columns_seq(
                    *[right_df[col].extend_constant(None, diff).alias(col) for col in right_df.columns]
                )
            else:
                diff = right_df_len - left_df_len
                left_df = pl.DataFrame().with_columns_seq(
                    *[left_df[col].extend_constant(None, diff).alias(col) for col in left_df.columns]
                )
        return left_df.hstack(right_df, in_place=False)
