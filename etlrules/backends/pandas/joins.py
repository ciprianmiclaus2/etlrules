from etlrules.backends.common.joins import (
    LeftJoinRule as LeftJoinRuleBase,
    RightJoinRule as RightJoinRuleBase,
    SemiJoinRule as SemiJoinRuleBase,
    AntiJoinRule as AntiJoinRuleBase,
    InnerJoinRule as InnerJoinRuleBase,
    OuterJoinRule as OuterJoinRuleBase,
)


class JoinsMixin():
    def do_apply(self, left_df, right_df):
        left_on, right_on = self._get_key_columns()
        return left_df.merge(
            right_df,
            how=self.JOIN_TYPE,
            left_on=left_on,
            right_on=right_on,
            suffixes=self.suffixes
        )


class LeftJoinRule(JoinsMixin, LeftJoinRuleBase):
    JOIN_TYPE = "left"


class InnerJoinRule(JoinsMixin, InnerJoinRuleBase):
    JOIN_TYPE = "inner"


class OuterJoinRule(JoinsMixin, OuterJoinRuleBase):
    JOIN_TYPE = "outer"


class RightJoinRule(JoinsMixin, RightJoinRuleBase):
    JOIN_TYPE = "right"


KEY_PREFIX = "__etlrules_key_"
MERGE_INDICATOR = "__etlrules_merge"


class FilterJoinMixin:
    """ Keeps (semi join) or removes (anti join) the left rows with a match in the right dataframe. """

    KEEP_MATCHES = None

    def do_apply(self, left_df, right_df):
        left_on, right_on = self._get_key_columns()
        keys = [f"{KEY_PREFIX}{idx}" for idx in range(len(right_on))]
        # missing keys never match (like SQL); unique keys so that the left rows are not duplicated
        right_keys = right_df[right_on].dropna().drop_duplicates().rename(columns=dict(zip(right_on, keys)))
        merged = left_df.merge(right_keys, how="left", left_on=left_on, right_on=keys, indicator=MERGE_INDICATOR)
        matched = merged[MERGE_INDICATOR] == "both"
        result = merged[matched if self.KEEP_MATCHES else ~matched]
        return result[list(left_df.columns)].reset_index(drop=True)


class SemiJoinRule(FilterJoinMixin, SemiJoinRuleBase):
    KEEP_MATCHES = True


class AntiJoinRule(FilterJoinMixin, AntiJoinRuleBase):
    KEEP_MATCHES = False
