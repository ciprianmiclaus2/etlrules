import dask
import dask.dataframe as dd
from typing import Mapping


class DaskMixin:
    def assign_do_apply(self, df: dd.DataFrame, input_column: str, output_column: str):
        return df.assign(**{output_column: self.do_apply(df, df[input_column])})

    def assign_do_apply_dict(self, df: dd.DataFrame, mapper_dict: Mapping[str, dd.Series]):
        return df.assign(**mapper_dict)


def force_pyarrow_string_config(value: bool):
    # when pyarrow[string] is enabled, dask cannot deal with objects
    # and stringifies the objects, which makes certain rules not work
    # properly, this is to force dask not to use pyarrow[string]
    # However, when the rules that are not impacted are not used, we
    # want to keep pyarrow[strings] as they are faster and use less mem
    dask.config.set({"dataframe.convert-string": bool(value)})


def is_pyarrow_string_enabled():
    return dask.config.get("dataframe.convert-string")
