from etlrules.backends.common.validation import (
    AssertNotNullRule as AssertNotNullRuleBase,
    AssertUniqueRule as AssertUniqueRuleBase,
)


class AssertNotNullRule(AssertNotNullRuleBase):

    def count_nulls(self, df, columns):
        return {col: df[col].null_count for col in columns}


class AssertUniqueRule(AssertUniqueRuleBase):

    def count_duplicates(self, df, columns):
        # a group_by without aggregations returns the unique combinations (missing values are grouped together)
        return df.num_rows - df.group_by(columns).aggregate([]).num_rows
