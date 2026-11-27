import bz2
import gzip
import io
import lzma
import os
import zipfile

import polars as pl

from etlrules.exceptions import MissingColumnError
from etlrules.backends.polars.types import MAP_TYPES

from etlrules.backends.common.io.files import (
    ReadCSVFileRule as ReadCSVFileRuleBase,
    ReadParquetFileRule as ReadParquetFileRuleBase,
    WriteCSVFileRule as WriteCSVFileRuleBase,
    WriteParquetFileRule as WriteParquetFileRuleBase,
    ReadJSONFileRule as ReadJSONFileRuleBase,
    WriteJSONFileRule as WriteJSONFileRuleBase,
)


# file extension -> compression
COMPRESSION_EXT = {
    '.zip': 'zip',
    '.gz': 'gzip',
    '.bz2': 'bz2',
    '.xz': 'xz',
}

# single file compressions (zip is an archive and is handled separately)
COMPRESSION_OPEN = {
    'gzip': gzip.open,
    'bz2': bz2.open,
    'xz': lzma.open,
}


class ConcatMixin:
    """ Concatenates the dataframes read from multiple files (regex=True); missing columns are filled with nulls. """

    def do_concat(self, left_df, right_df):
        return pl.concat([left_df, right_df], how="diagonal_relaxed")


def _decompressed_source(file_path: str, is_uri: bool):
    """ A source polars can read: the path, or the decompressed bytes for URIs and bz2/xz/zip files. """
    if is_uri:
        import urllib.request
        with urllib.request.urlopen(file_path) as response:
            return io.BytesIO(response.read())
    _, ext = os.path.splitext(file_path)
    compression = COMPRESSION_EXT.get(ext.lower())
    if compression == "zip" or (compression is not None and zipfile.is_zipfile(file_path)):
        with zipfile.ZipFile(file_path, "r") as archive:
            names = archive.namelist()
            if len(names) != 1:
                raise RuntimeError(f"Only a single file can be read from an archive. {file_path} has {len(names)} files.")
            return io.BytesIO(archive.read(names[0]))
    if compression is not None:
        with COMPRESSION_OPEN[compression](file_path, "rb") as f:
            return io.BytesIO(f.read())
    return file_path


class ReadCSVFileRule(ConcatMixin, ReadCSVFileRuleBase):

    def _read_csv(self, source) -> pl.DataFrame:
        return pl.read_csv(
            source, separator=self.separator, has_header=self.header,
            skip_rows=self.skip_header_rows or 0
        )

    def _read_zip(self, file_path: str) -> pl.DataFrame:
        with zipfile.ZipFile(file_path, 'r') as zarch:
            arch_files = zarch.namelist()
            if len(arch_files) != 1:
                raise RuntimeError(f"Only a single csv file can be read from an archive. {file_path} has {len(arch_files)} files.")
            with zarch.open(arch_files[0]) as zf:
                return self._read_csv(zf.read())

    def do_read(self, file_path: str) -> pl.DataFrame:
        _, ext = os.path.splitext(file_path)
        compression = COMPRESSION_EXT.get(ext.lower())
        if compression is None or self._is_uri():
            return self._read_csv(file_path)
        if compression == 'zip' or zipfile.is_zipfile(file_path):
            # etlrules < 0.3.5 wrote zip archives for all the compressions with the polars backend
            return self._read_zip(file_path)
        with COMPRESSION_OPEN[compression](file_path, 'rb') as f:
            return self._read_csv(f.read())


class ReadParquetFileRule(ConcatMixin, ReadParquetFileRuleBase):
    def do_read(self, file_path: str) -> pl.DataFrame:
        from pyarrow.lib import ArrowInvalid
        try:
            return pl.read_parquet(
                file_path, use_pyarrow=True, columns=self.columns,
                pyarrow_options={
                    "filters": self.filters
                }
            )
        except ArrowInvalid as exc:
            raise MissingColumnError(str(exc))


class WriteCSVFileRule(WriteCSVFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pl.DataFrame) -> None:
        file_path = os.path.join(file_dir, file_name)
        if self.compression == 'zip':
            # name the csv file inside the archive like pandas does: output.csv.zip -> output.csv
            fname_csv, _ = os.path.splitext(file_name)
            with zipfile.ZipFile(file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zarch:
                with zarch.open(fname_csv, "w") as zf:
                    df.write_csv(zf, separator=self.separator, include_header=self.header)
        elif self.compression is not None:
            with COMPRESSION_OPEN[self.compression](file_path, 'wb') as f:
                df.write_csv(f, separator=self.separator, include_header=self.header)
        else:
            df.write_csv(file_path, separator=self.separator, include_header=self.header)


class WriteParquetFileRule(WriteParquetFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pl.DataFrame) -> None:
        df.write_parquet(
            os.path.join(file_dir, file_name),
            use_pyarrow=True,
            compression=self.compression or "uncompressed",
        )


class ReadJSONFileRule(ConcatMixin, ReadJSONFileRuleBase):

    def do_read(self, file_path: str) -> pl.DataFrame:
        # infer the types from all the rows (polars only looks at the first 100 rows by default)
        df = pl.read_ndjson(_decompressed_source(file_path, self._is_uri()), infer_schema_length=None)
        column_types = self._get_column_types(df.columns)
        if column_types:
            df = df.with_columns(pl.col(col).cast(MAP_TYPES[col_type]) for col, col_type in column_types.items())
        return df


class WriteJSONFileRule(WriteJSONFileRuleBase):

    def do_write(self, file_name: str, file_dir: str, df: pl.DataFrame) -> None:
        # the same datetime format in all the backends: 2023-05-19T10:30:45.000000
        df = df.with_columns(
            pl.col(col).dt.strftime("%Y-%m-%dT%H:%M:%S%.6f") for col, dtype in df.schema.items() if dtype == pl.Datetime
        )
        file_path = os.path.join(file_dir, file_name)
        if self.compression == "zip":
            inner_name, _ = os.path.splitext(file_name)
            with zipfile.ZipFile(file_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(inner_name, df.write_ndjson())
        elif self.compression is not None:
            with COMPRESSION_OPEN[self.compression](file_path, "wb") as f:
                df.write_ndjson(f)
        else:
            df.write_ndjson(file_path)
