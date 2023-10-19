import os, re
import pandas as pd
from typing import List, Optional, Sequence, Tuple, Union

from etlrules.exceptions import MissingColumnError
from etlrules.rule import BaseRule, UnaryOpBaseRule


class BaseReadFileRule(BaseRule):
    def __init__(self, file_name, file_dir=".", regex=False, named_output=None, name=None, description=None, strict=True):
        super().__init__(named_output=named_output, name=name, description=description, strict=strict)
        self.file_name = file_name
        self.file_dir = file_dir
        self.regex = bool(regex)

    def _get_full_file_paths(self):
        if self.regex:
            pattern = re.compile(self.file_name)
            for file_name in os.listdir(self.file_dir):
                if pattern.match(file_name):
                    yield os.path.join(self.file_dir, file_name)
        else:
            yield os.path.join(self.file_dir, self.file_name)

    def do_read(self, file_path: str) -> pd.DataFrame:
        raise NotImplementedError()

    def apply(self, data):
        super().apply(data)

        result = None
        for file_path in self._get_full_file_paths():
            df = self.do_read(file_path)
            if result is None:
                result = df
            else:
                result = pd.concat([result, df], axis=0, ignore_index=True)
        self._set_output_df(data, result)


class ReadCSVFileRule(BaseReadFileRule):
    def __init__(self, file_name, file_dir=".", regex=False, separator=",", header=True, named_output=None, name=None, description=None, strict=True):
        super().__init__(file_name=file_name, file_dir=file_dir, regex=regex, named_output=named_output, name=name, description=description, strict=strict)
        self.separator = separator
        self.header = header

    def do_read(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(
            file_path, sep=self.separator, header='infer' if self.header else None,
            index_col=False
        )


class ReadParquetFileRule(BaseReadFileRule):

    SUPPORTED_FILTERS_OPS = {"==", "=", ">", ">=", "<", "<=", "!=", "in", "not in"}

    def __init__(self, file_name: str, file_dir: str=".", columns: Optional[Sequence[str]]=None, filters:Optional[Union[List[Tuple], List[List[Tuple]]]]=None, regex: bool=False, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(
            file_name=file_name, file_dir=file_dir, regex=regex, named_output=named_output,
            name=name, description=description, strict=strict)
        self.columns = columns
        self.filters = filters
        if self.filters is not None and not self._is_valid_filters():
            raise ValueError("Invalid filters. It must be a List[Tuple] or List[List[Tuple]] with each Tuple being (column, op, value).")

    def _is_valid_filter_tuple(self, tpl):
        valid = (
            isinstance(tpl, tuple) and len(tpl) == 3 and tpl[0] and isinstance(tpl[0], str) and
                tpl[1] in self.SUPPORTED_FILTERS_OPS
        )
        if valid and tpl[1] in ('in', 'not in'):
            valid = isinstance(tpl[2], (list, tuple, set))
        return valid

    def _is_valid_filters(self):
        if isinstance(self.filters, list):
            if all(isinstance(elem, list) for elem in self.filters):
                # List[List[Tuple]]
                return all(
                    self._is_valid_filter_tuple(tpl)
                    for elem in self.filters
                    for tpl in elem
                )
            elif all(isinstance(elem, tuple) for elem in self.filters):
                # List[Tuple]
                return all(self._is_valid_filter_tuple(tpl) for tpl in self.filters)
        return False

    def do_read(self, file_path: str) -> pd.DataFrame:
        from pyarrow.lib import ArrowInvalid
        try:
            return pd.read_parquet(
                file_path, engine="pyarrow", columns=self.columns, filters=self.filters
            )
        except ArrowInvalid as exc:
            raise MissingColumnError(str(exc))


class BaseWriteFileRule(UnaryOpBaseRule):
    def __init__(self, file_name, file_dir=".", named_input=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=None, name=name, description=description, strict=strict)
        self.file_name = file_name
        self.file_dir = file_dir

    def do_write(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        self.do_write(df)


class WriteCSVFileRule(BaseWriteFileRule):

    COMPRESSIONS = {
        'zip': '.zip',
        'gzip': '.gz',
        'bz2': '.bz2',
        #'zstd': '.zst',
        'xz': '.xz',
    }

    def __init__(self, file_name, file_dir=".", separator=",", header=True, compression=None, named_input=None, name=None, description=None, strict=True):
        super().__init__(
            file_name=file_name, file_dir=file_dir, named_input=named_input, 
            name=name, description=description, strict=strict)
        self.separator = separator
        self.header = header
        assert compression is None or compression in self.COMPRESSIONS.keys(), f"Unsupported compression '{compression}'. It must be one of: {self.COMPRESSIONS.keys()}."
        if compression:
            assert file_name.endswith(self.COMPRESSIONS[compression]), f"The file name {file_name} must have the extension {self.COMPRESSIONS[compression]} when the compression is set to {compression}."
        self.compression = compression

    def do_write(self, df: pd.DataFrame) -> None:
        df.to_csv(
            os.path.join(self.file_dir, self.file_name),
            sep=self.separator,
            header=self.header,
            compression=self.compression,
            index=False,
        )


class WriteParquetFileRule(BaseWriteFileRule):

    COMPRESSIONS = ("snappy", "gzip", "brotli", "lz4", "zstd")

    def __init__(self, file_name, file_dir=".", compression=None, named_input=None, name=None, description=None, strict=True):
        super().__init__(
            file_name=file_name, file_dir=file_dir, named_input=named_input, 
            name=name, description=description, strict=strict)
        assert compression is None or compression in self.COMPRESSIONS, f"Unsupported compression '{compression}'. It must be one of: {self.COMPRESSIONS}."
        self.compression = compression

    def do_write(self, df: pd.DataFrame) -> None:
        df.to_parquet(
            path=os.path.join(self.file_dir, self.file_name),
            engine="pyarrow",
            compression=self.compression,
            index=False
        )

