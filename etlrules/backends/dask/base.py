import dask.dataframe as dd
from typing import Mapping


class DaskMixin:
    def assign_do_apply(self, df: dd.DataFrame, input_column: str, output_column: str):
        return df.assign(**{output_column: self.do_apply(df, df[input_column])})

    def assign_do_apply_dict(self, df: dd.DataFrame, mapper_dict: Mapping[str, dd.Series]):
        return df.assign(**mapper_dict)
