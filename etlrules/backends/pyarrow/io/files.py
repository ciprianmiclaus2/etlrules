import io
import lzma
import os
import urllib.request
import zipfile

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from etlrules.exceptions import MissingColumnError
from etlrules.backends.common.io.files import (
    ReadCSVFileRule as ReadCSVFileRuleBase,
    ReadParquetFileRule as ReadParquetFileRuleBase,
    WriteCSVFileRule as WriteCSVFileRuleBase,
    WriteParquetFileRule as WriteParquetFileRuleBase,
)
from etlrules.backends.pyarrow.concat import concat_tables


# compressions supported by pyarrow's streams (the others use python's modules)
ARROW_COMPRESSIONS = {
    'gzip': 'gzip',
    'bz2': 'bz2',
}


def _read_source(file_path: str, is_uri: bool):
    """ Returns a source pyarrow's csv reader can read: a path, or a buffer for URIs, xz and zip files. """
    if is_uri:
        with urllib.request.urlopen(file_path) as response:
            return pa.BufferReader(response.read())
    lowered = file_path.lower()
    if lowered.endswith(".xz"):
        with lzma.open(file_path, "rb") as f:
            return pa.BufferReader(f.read())
    if lowered.endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as archive:
            names = archive.namelist()
            if len(names) != 1:
                raise RuntimeError(f"Only a single csv file can be read from an archive. {file_path} has {len(names)} files.")
            return pa.BufferReader(archive.read(names[0]))
    # gzip and bz2 are detected from the extension by pyarrow
    return file_path


class ReadCSVFileRule(ReadCSVFileRuleBase):

    def do_read(self, file_path: str) -> pa.Table:
        read_options = pacsv.ReadOptions(
            skip_rows=self.skip_header_rows or 0,
            autogenerate_column_names=not self.header,
        )
        parse_options = pacsv.ParseOptions(delimiter=self.separator)
        # like the other backends, empty values are nulls (also for strings)
        convert_options = pacsv.ConvertOptions(strings_can_be_null=True)
        return pacsv.read_csv(
            _read_source(file_path, self._is_uri()),
            read_options=read_options, parse_options=parse_options, convert_options=convert_options,
        )

    def do_concat(self, left_df, right_df):
        return concat_tables([left_df, right_df])


class ReadParquetFileRule(ReadParquetFileRuleBase):

    def do_read(self, file_path: str) -> pa.Table:
        try:
            return pq.read_table(file_path, columns=self.columns, filters=self.filters)
        except (pa.ArrowInvalid, KeyError) as exc:
            raise MissingColumnError(str(exc)) from exc

    def do_concat(self, left_df, right_df):
        return concat_tables([left_df, right_df])


class WriteCSVFileRule(WriteCSVFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pa.Table) -> None:
        file_path = os.path.join(file_dir, file_name)
        write_options = pacsv.WriteOptions(include_header=self.header, delimiter=self.separator)
        if self.compression in ARROW_COMPRESSIONS:
            with pa.CompressedOutputStream(file_path, ARROW_COMPRESSIONS[self.compression]) as out:
                pacsv.write_csv(df, out, write_options=write_options)
        elif self.compression == "xz":
            with lzma.open(file_path, "wb") as out:
                pacsv.write_csv(df, out, write_options=write_options)
        elif self.compression == "zip":
            buffer = io.BytesIO()
            pacsv.write_csv(df, buffer, write_options=write_options)
            # name the csv file inside the archive like pandas does: output.csv.zip -> output.csv
            inner_name, _ = os.path.splitext(file_name)
            with zipfile.ZipFile(file_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(inner_name, buffer.getvalue())
        else:
            pacsv.write_csv(df, file_path, write_options=write_options)


class WriteParquetFileRule(WriteParquetFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pa.Table) -> None:
        pq.write_table(df, os.path.join(file_dir, file_name), compression=self.compression or "none")
