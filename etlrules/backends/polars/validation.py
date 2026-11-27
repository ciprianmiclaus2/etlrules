from etlrules.backends.common.validation import (
    AssertNotNullRule as AssertNotNullRuleBase,
    AssertUniqueRule as AssertUniqueRuleBase,
)


class AssertNotNullRule(AssertNotNullRuleBase):

    def count_nulls(self, df, columns):
        return {col: df[col].null_count() for col in columns}


class AssertUniqueRule(AssertUniqueRuleBase):

    def count_duplicates(self, df, columns):
        if df.height == 0:
            return 0
        return df.height - df.select(columns).n_unique()
