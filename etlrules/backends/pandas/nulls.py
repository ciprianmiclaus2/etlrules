from etlrules.backends.common.nulls import (
    CoalesceRule as CoalesceRuleBase,
    DropNullRowsRule as DropNullRowsRuleBase,
    FillNullRule as FillNullRuleBase,
)


class FillNullRule(FillNullRuleBase):

    def do_fill(self, df):
        return df.assign(**{col: df[col].fillna(self.value) for col in self.columns})


class DropNullRowsRule(DropNullRowsRuleBase):

    def do_drop(self, df, columns):
        return df.dropna(subset=columns, how=self.how, ignore_index=True)


class CoalesceRule(CoalesceRuleBase):

    def do_coalesce(self, df):
        result = df[self.columns[0]]
        for col in self.columns[1:]:
            result = result.fillna(df[col])
        return df.assign(**{self.output_column: result})
