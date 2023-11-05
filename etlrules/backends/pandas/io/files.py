import os
import pandas as pd

from etlrules.exceptions import MissingColumnError

from etlrules.backends.common.io.files import (
    ReadCSVFileRule as ReadCSVFileRuleBase,
    ReadParquetFileRule as ReadParquetFileRuleBase,
    WriteCSVFileRule as WriteCSVFileRuleBase,
    WriteParquetFileRule as WriteParquetFileRuleBase,
)


class ReadCSVFileRule(ReadCSVFileRuleBase):
    def do_read(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(
            file_path, sep=self.separator, header='infer' if self.header else None,
            index_col=False
        )


class ReadParquetFileRule(ReadParquetFileRuleBase):
    def do_read(self, file_path: str) -> pd.DataFrame:
        from pyarrow.lib import ArrowInvalid
        try:
            return pd.read_parquet(
                file_path, engine="pyarrow", columns=self.columns, filters=self.filters
            )
        except ArrowInvalid as exc:
            raise MissingColumnError(str(exc))


class WriteCSVFileRule(WriteCSVFileRuleBase):

    def do_write(self, df: pd.DataFrame) -> None:
        df.to_csv(
            os.path.join(self.file_dir, self.file_name),
            sep=self.separator,
            header=self.header,
            compression=self.compression,
            index=False,
        )


class WriteParquetFileRule(WriteParquetFileRuleBase):

    def do_write(self, df: pd.DataFrame) -> None:
        df.to_parquet(
            path=os.path.join(self.file_dir, self.file_name),
            engine="pyarrow",
            compression=self.compression,
            index=False
        )

