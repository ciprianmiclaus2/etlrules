import bz2
import gzip
import lzma
import os
import zipfile

import polars as pl

from etlrules.exceptions import MissingColumnError

from etlrules.backends.common.io.files import (
    ReadCSVFileRule as ReadCSVFileRuleBase,
    ReadParquetFileRule as ReadParquetFileRuleBase,
    WriteCSVFileRule as WriteCSVFileRuleBase,
    WriteParquetFileRule as WriteParquetFileRuleBase,
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


class ReadCSVFileRule(ReadCSVFileRuleBase):

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


class ReadParquetFileRule(ReadParquetFileRuleBase):
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
