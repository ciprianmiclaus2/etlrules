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


MATCH_COLUMN = "__etlrules_match"


class FilterJoinMixin:
    """ Keeps (semi join) or removes (anti join) the left rows with a match in the right dataframe. """

    KEEP_MATCHES = None

    def do_apply(self, left_df, right_df):
        left_on, right_on = self._get_key_columns()
        # missing keys never match (like SQL); unique keys so that the left rows are not duplicated
        # (the columns are renamed before drop_duplicates: renaming after it trips dask's optimizer)
        right_keys = right_df[right_on].rename(columns=dict(zip(right_on, left_on))).dropna().drop_duplicates()
        # a single partition broadcast to all the left partitions keeps the order of the left rows
        right_keys = right_keys.repartition(npartitions=1).assign(**{MATCH_COLUMN: True})
        merged = left_df.merge(right_keys, how="left", on=left_on, broadcast=True)
        matched = merged[MATCH_COLUMN].notnull()
        result = merged[matched if self.KEEP_MATCHES else ~matched]
        return result[list(left_df.columns)].reset_index(drop=True)


class SemiJoinRule(FilterJoinMixin, SemiJoinRuleBase):
    KEEP_MATCHES = True


class AntiJoinRule(FilterJoinMixin, AntiJoinRuleBase):
    KEEP_MATCHES = False
