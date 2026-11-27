from etlrules.backends.common.validation import (
    AssertNotNullRule as AssertNotNullRuleBase,
    AssertUniqueRule as AssertUniqueRuleBase,
)


class AssertNotNullRule(AssertNotNullRuleBase):

    def count_nulls(self, df, columns):
        return {col: df[col].isna().sum() for col in columns}


class AssertUniqueRule(AssertUniqueRuleBase):

    def count_duplicates(self, df, columns):
        return df.duplicated(subset=columns).sum()
