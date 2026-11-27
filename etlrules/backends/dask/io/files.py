import bz2
import contextlib
import gzip
import io
import lzma
import os
import zipfile

import dask.dataframe as dd

from etlrules.exceptions import MissingColumnError
from etlrules.backends.dask.types import MAP_TYPES

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
        return dd.concat([left_df, right_df]).reset_index(drop=True)


@contextlib.contextmanager
def open_compressed(file_path: str, compression, inner_name: str):
    """ Opens file_path for writing bytes, compressed with gzip/bz2/xz or in a zip archive (as inner_name). """
    if compression == "zip":
        buffer = io.BytesIO()
        yield buffer
        with zipfile.ZipFile(file_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(inner_name, buffer.getvalue())
    else:
        openers = {None: open, "gzip": gzip.open, "bz2": bz2.open, "xz": lzma.open}
        with openers[compression](file_path, "wb") as f:
            yield f


class ReadCSVFileRule(ConcatMixin, ReadCSVFileRuleBase):
    def do_read(self, file_path: str) -> dd.DataFrame:
        return dd.read_csv(
            file_path, blocksize=None, sep=self.separator, header='infer' if self.header else None,
            skiprows=self.skip_header_rows,
            index_col=False
        )


def parquet_file_name_split(file_name: str) -> tuple[str, str]:
    """ Splits a file name in the name and the extension (with the dot), e.g. data.parquet -> data, .parquet """
    fn, ext = os.path.splitext(file_name)
    return fn, ext or ".parquet"


class ReadParquetFileRule(ConcatMixin, ReadParquetFileRuleBase):

    def do_read(self, file_path: str) -> dd.DataFrame:
        from pyarrow.lib import ArrowInvalid
        file_dir, file_name = os.path.split(file_path)
        fn, ext = parquet_file_name_split(file_name)
        try:
            return dd.read_parquet(
                os.path.join(file_dir, f"{fn}*{ext}"), engine="pyarrow", columns=self.columns, filters=self.filters
            )
        except ArrowInvalid as exc:
            raise MissingColumnError(str(exc))
        except ValueError as exc:
            if "The following columns were not found in the dataset" in str(exc):
                raise MissingColumnError(str(exc))
            raise
        except KeyError as exc:
            # newer dask versions: KeyError("... ['M'] not in index")
            if "not in index" in str(exc):
                raise MissingColumnError(str(exc))
            raise


class WriteCSVFileRule(WriteCSVFileRuleBase):

    def do_write(self, file_name: str, file_dir: str,  df: dd.DataFrame) -> None:
        df.to_csv(
            os.path.join(file_dir, file_name),
            single_file=True,
            sep=self.separator,
            header=self.header,
            compression=self.compression,
            index=False,
        )


class WriteParquetFileRule(WriteParquetFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: dd.DataFrame) -> None:
        fn, ext = parquet_file_name_split(file_name)
        df.to_parquet(
            path=file_dir,
            engine="pyarrow",
            write_metadata_file=False,
            name_function=lambda idx: f"{fn}_part_{idx}{ext}",
            compression=self.compression,
            write_index=False
        )


class ReadJSONFileRule(ConcatMixin, ReadJSONFileRuleBase):

    def do_read(self, file_path: str) -> dd.DataFrame:
        from etlrules.backends.pandas.io.files import READ_JSON_OPTIONS
        # one partition per file, like the csv reader
        df = dd.read_json(file_path, blocksize=None, compression="infer", **READ_JSON_OPTIONS)
        column_types = self._get_column_types(df.columns)
        if column_types:
            df = df.astype({col: MAP_TYPES[col_type] for col, col_type in column_types.items()})
        return df


class WriteJSONFileRule(WriteJSONFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: dd.DataFrame) -> None:
        from etlrules.backends.pandas.io.files import WRITE_JSON_OPTIONS
        # a single file (like the other backends): the partitions are computed and written one at a time
        inner_name, _ = os.path.splitext(file_name)
        with open_compressed(os.path.join(file_dir, file_name), self.compression, inner_name) as f:
            for idx in range(df.npartitions):
                text = df.get_partition(idx).compute().to_json(**WRITE_JSON_OPTIONS)
                if text and not text.endswith("\n"):
                    text += "\n"
                f.write(text.encode("utf-8"))
