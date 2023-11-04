import polars as pl
from typing import Mapping


class PolarsMixin:
    def assign_do_apply(self, df, input_column, output_column):
        return df.with_columns_seq(
            self.do_apply(df, df[input_column]).alias(output_column)
        )

    def assign_do_apply_dict(self, df: pl.DataFrame, mapper_dict: Mapping[str, pl.Series]):
        return df.with_columns(
            col.alias(col_name) for col_name, col in mapper_dict.items()
        )
