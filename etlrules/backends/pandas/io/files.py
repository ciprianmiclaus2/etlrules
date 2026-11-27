import os

import pandas as pd

from etlrules.exceptions import MissingColumnError
from etlrules.backends.pandas.types import MAP_TYPES

from etlrules.backends.common.io.files import (
    ReadCSVFileRule as ReadCSVFileRuleBase,
    ReadParquetFileRule as ReadParquetFileRuleBase,
    WriteCSVFileRule as WriteCSVFileRuleBase,
    WriteParquetFileRule as WriteParquetFileRuleBase,
    ReadJSONFileRule as ReadJSONFileRuleBase,
    WriteJSONFileRule as WriteJSONFileRuleBase,
)


class ConcatMixin:
    """ Concatenates the dataframes read from multiple files (regex=True); missing columns are filled with NA. """

    def do_concat(self, left_df, right_df):
        return pd.concat([left_df, right_df], ignore_index=True)


# read_json options: nullable types (e.g. Int64 for ints with nulls) and no conversion of the strings to dates
# (pandas 2 needs convert_dates=False for the columns named like dates; the keyword is deprecated in pandas 3)
READ_JSON_OPTIONS = {"lines": True, "orient": "records", "dtype_backend": "numpy_nullable", "dtype": False}
if int(pd.__version__.split(".")[0]) < 3:
    READ_JSON_OPTIONS["convert_dates"] = False

WRITE_JSON_OPTIONS = {"orient": "records", "lines": True, "date_format": "iso", "date_unit": "us", "force_ascii": False, "double_precision": 15}


class ReadCSVFileRule(ConcatMixin, ReadCSVFileRuleBase):
    def do_read(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(
            file_path, sep=self.separator, header='infer' if self.header else None,
            skiprows=self.skip_header_rows,
            index_col=False
        )


class ReadParquetFileRule(ConcatMixin, ReadParquetFileRuleBase):
    def do_read(self, file_path: str) -> pd.DataFrame:
        from pyarrow.lib import ArrowInvalid
        try:
            return pd.read_parquet(
                file_path, engine="pyarrow", columns=self.columns, filters=self.filters
            )
        except ArrowInvalid as exc:
            raise MissingColumnError(str(exc))


class WriteCSVFileRule(WriteCSVFileRuleBase):

    def do_write(self, file_name: str, file_dir: str,  df: pd.DataFrame) -> None:
        df.to_csv(
            os.path.join(file_dir, file_name),
            sep=self.separator,
            header=self.header,
            compression=self.compression,
            index=False,
        )


class WriteParquetFileRule(WriteParquetFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pd.DataFrame) -> None:
        df.to_parquet(
            path=os.path.join(file_dir, file_name),
            engine="pyarrow",
            compression=self.compression,
            index=False
        )


class ReadJSONFileRule(ConcatMixin, ReadJSONFileRuleBase):

    def do_read(self, file_path: str) -> pd.DataFrame:
        df = pd.read_json(file_path, **READ_JSON_OPTIONS)
        column_types = self._get_column_types(df.columns)
        if column_types:
            df = df.astype({col: MAP_TYPES[col_type] for col, col_type in column_types.items()})
        return df


class WriteJSONFileRule(WriteJSONFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pd.DataFrame) -> None:
        df.to_json(os.path.join(file_dir, file_name), compression=self.compression, **WRITE_JSON_OPTIONS)
