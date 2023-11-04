import pandas as pd


class PandasMixin:
    def assign_do_apply(self, df: pd.DataFrame, input_column: str, output_column: str):
        return df.assign(**{output_column: self.do_apply(df, df[input_column])})
