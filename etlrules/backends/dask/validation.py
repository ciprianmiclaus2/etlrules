import dask

from etlrules.backends.common.validation import (
    AssertNotNullRule as AssertNotNullRuleBase,
    AssertUniqueRule as AssertUniqueRuleBase,
)


class AssertNotNullRule(AssertNotNullRuleBase):

    def count_nulls(self, df, columns):
        counts = dask.compute(*[df[col].isna().sum() for col in columns])
        return dict(zip(columns, counts))


class AssertUniqueRule(AssertUniqueRuleBase):

    def count_duplicates(self, df, columns):
        num_rows, num_unique = dask.compute(df.shape[0], df[columns].drop_duplicates().shape[0])
        return num_rows - num_unique
