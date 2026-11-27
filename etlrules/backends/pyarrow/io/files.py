import bz2
import contextlib
import datetime
import decimal
import gzip
import io
import json
import lzma
import os
import urllib.request
import zipfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as pq

from etlrules.exceptions import MissingColumnError
from etlrules.backends.common.io.files import (
    ReadCSVFileRule as ReadCSVFileRuleBase,
    ReadParquetFileRule as ReadParquetFileRuleBase,
    WriteCSVFileRule as WriteCSVFileRuleBase,
    WriteParquetFileRule as WriteParquetFileRuleBase,
    ReadJSONFileRule as ReadJSONFileRuleBase,
    WriteJSONFileRule as WriteJSONFileRuleBase,
)
from etlrules.backends.pyarrow.concat import concat_tables
from etlrules.backends.pyarrow.types import MAP_TYPES


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


class ReadJSONFileRule(ReadJSONFileRuleBase):

    def do_read(self, file_path: str) -> pa.Table:
        source = _read_source(file_path, self._is_uri())
        if isinstance(source, pa.BufferReader):
            data = source.read_buffer()
            make_source = lambda: pa.BufferReader(data)  # noqa: E731
        else:
            make_source = lambda: source  # noqa: E731
        table = pajson.read_json(make_source())
        # pyarrow converts the strings which look like dates to timestamps: read them as strings, like the
        # other backends (they can be converted with StrToDateTimeRule)
        timestamps = [field.name for field in table.schema if pa.types.is_timestamp(field.type)]
        if timestamps:
            parse_options = pajson.ParseOptions(
                explicit_schema=pa.schema([(col, pa.string()) for col in timestamps]),
                unexpected_field_behavior="infer",
            )
            columns = table.column_names
            # the columns in the explicit schema come first: keep the order of the columns in the file
            table = pajson.read_json(make_source(), parse_options=parse_options).select(columns)
        for col, col_type in self._get_column_types(table.column_names).items():
            table = table.set_column(table.column_names.index(col), col, pc.cast(table[col], MAP_TYPES[col_type]))
        return table

    def do_concat(self, left_df, right_df):
        return concat_tables([left_df, right_df])


def _json_default(value):
    # the same datetime format in all the backends: 2023-05-19T10:30:45.000000
    if isinstance(value, datetime.datetime):
        return value.strftime(WriteJSONFileRuleBase.DATETIME_FORMAT)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class WriteJSONFileRule(WriteJSONFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pa.Table) -> None:
        # pyarrow has no JSON writer: the rows are written with python's json module
        inner_name, _ = os.path.splitext(file_name)
        with open_compressed(os.path.join(file_dir, file_name), self.compression, inner_name) as f:
            for batch in df.to_batches(max_chunksize=10_000):
                lines = "".join(
                    json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=_json_default) + "\n"
                    for row in batch.to_pylist()
                )
                f.write(lines.encode("utf-8"))
