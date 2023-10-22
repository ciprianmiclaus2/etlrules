import os, re
import pandas as pd
from typing import List, Optional, Sequence, Tuple, Union

from etlrules.exceptions import MissingColumnError
from etlrules.rule import BaseRule, UnaryOpBaseRule


class BaseReadFileRule(BaseRule):
    def __init__(self, file_name: str, file_dir: str=".", regex: bool=False, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
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
    r""" Reads one or multiple csv files from a directory and persists it as a dataframe for subsequent rules to operate on.

    Basic usage::

        # reads a file data.csv and persists it as the main output of the rule
        rule = ReadCSVFileRule("data.csv", "/home/myuser/")
        rule.apply(data)

        # reads a file test_data.csv and persists it as the input_data named output
        # other rules can specify input_data as their named_input to operate on it
        rule = ReadCSVFileRule("test_data.csv", "/home/myuser/", named_output="input_data")
        rule.apply(data)

        # extracts all files starting with data followed by 4 digits and concatenate them
        # e.g. data1234.csv, data5678.csv, etc.
        rule = ReadCSVFileRule("data[0-9]{4}.csv", "/home/myuser/", regex=True, named_output="input_data")
        rule.apply(data)

    Args:
        file_name: The name of the csv file to load. The format will be inferred from the extension of the file.
            A simple text csv file will be inferred from the .csv extension. The extensions like .zip, .gz, .bz2, .xz
            will extract a single compressed csv file from the given input compressed file.
            file_name can also be a regular expression (specify regex=True in that case).
            The reader will find all the files in the file_dir directory that match the regular expression and extract
            all those csv file and concatenate them into a single dataframe.
            For example, file_name=".*\.csv", file_dir=".", regex=True will extract all the files with the .csv extension
            from the current directory.
        file_dir: The file directory where the file_name is located. When file_name is a regular expression and 
            the regex parameter is True, file_dir is the directory that is inspected for any files that match the
            regular expression.
            Defaults to . (ie the current directory).
        regex: When True, the file_name is interpreted as a regular expression. Defaults to False.
        separator: The single character to be used as separator in the csv file. Defaults to , (comma).
        header: When True, the first line is interpreted as the header and the column names are extracted from it.
            When False, the first line is part of the data and the columns will have names like 0, 1, 2, etc.
            Defaults to True.

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        IOError: raised when the file is not found.
    """

    def __init__(self, file_name: str, file_dir: str=".", regex: bool=False, separator: str=",", header: bool=True, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(file_name=file_name, file_dir=file_dir, regex=regex, named_output=named_output, name=name, description=description, strict=strict)
        self.separator = separator
        self.header = header

    def do_read(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(
            file_path, sep=self.separator, header='infer' if self.header else None,
            index_col=False
        )


class ReadParquetFileRule(BaseReadFileRule):
    r""" Reads one or multiple parquet files from a directory and persists it as a dataframe for subsequent rules to operate on.

    Basic usage::

        # reads a file data.csv and persists it as the main output of the rule
        rule = ReadParquetFileRule("data.parquet", "/home/myuser/")
        rule.apply(data)

        # reads a file test_data.parquet and persists it as the input_data named output
        # other rules can specify input_data as their named_input to operate on it
        rule = ReadParquetFileRule("test_data.parquet", "/home/myuser/", named_output="input_data")
        rule.apply(data)

    Args:
        file_name: The name of the parquet file to load. The format will be inferred from the extension of the file.
            file_name can also be a regular expression (specify regex=True in that case).
            The reader will find all the files in the file_dir directory that match the regular expression and extract
            all those parquet file and concatenate them into a single dataframe.
            For example, file_name=".*\.parquet", file_dir=".", regex=True will extract all the files with the .parquet extension
            from the current directory.
        file_dir: The file directory where the file_name is located. When file_name is a regular expression and 
            the regex parameter is True, file_dir is the directory that is inspected for any files that match the
            regular expression.
            Defaults to . (ie the current directory).
        regex: When True, the file_name is interpreted as a regular expression. Defaults to False.
        columns: A subset of the columns in the parquet file to load.
        filters: A list of filters to apply to filter the rows returned. Rows which do not match the filter conditions
            will be removed from scanned data.
            When passed as a List[List[Tuple]], the conditions in the inner lists are AND-end together with the top level
            condition OR-ed together. Eg: ((cond1 AND cond2...) OR (cond3 AND cond4...)...)
            When passed as a List[Tuple], the conditions are AND-ed together. E.g.: cond1 AND cond2 AND cond3...
            Each condition is specified as a tuple of 3 elements: (column, operation, value).
            Column is the name of a column in the input dataframe.
            Operation is one of: "==", "=", ">", ">=", "<", "<=", "!=", "in", "not in".
            Value is a scalar value, int, float, string, etc. When the operation is in or not in, the value must be a list, tuple or set of values.

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        IOError: raised when the file is not found.
        ValueError: raised if filters are specified but the format is incorrect.
        MissingColumnError: raised if a column is specified in columns or filters but it doesn't exist in the input dataframe.
    
    Note:
        The parquet file can be compressed in which case the compression will be inferred from the file.
        The following compression algorithms are supported: "snappy", "gzip", "brotli", "lz4", "zstd".
    """

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
    """ Writes an existing dataframe to a csv file (optionally compressed) on disk.

    The rule is a final rule, which means it produces no additional outputs, it takes any of the existing outputs and writes it to disk.

    Basic usage::

        # writes a file data.csv and persists the main output of the previous rule to it
        rule = WriteCSVFileRule("data.csv", "/home/myuser/")
        rule.apply(data)

        # writes a file test_data.csv and persists the dataframe named input_data into it
        rule = WriteCSVFileRule("test_data.csv", "/home/myuser/", named_input="input_data")
        rule.apply(data)

    Args:
        file_name: The name of the csv file to write to disk. It will be written in the directory
            specified by the file_dir parameter.
        file_dir: The file directory where the file_name should be written.
            Defaults to . (ie the current directory).
        separator: The single character to separate values in the csv file. Defaults to , (comma).
        header: When True, the first line will contain the columns separated by the separator.
            When False, the columns will not be written and the first line contains data.
            Defaults to True.
        compression: Compress the csv file using a supported compression algorithms. Optional.
            When the compression is specified, the file_name must end with the extension associate with that
            compression format. The following options are supported:
            zip - file_name must end with .zip (e.g. output.csv.zip), will produced a zipped csv file
            gzip - file_name must end with .gz (e.g. output.csv.gz), will produced a gzipped csv file
            bz2 - file_name must end with .bz2 (e.g. output.csv.bz2), will produced a bzipped csv 
            xz - file_name must end with .xz (e.g. output.csv.xz), will produced a xz-compressed csv file

        named_input (Optional[str]): Select by name the dataframe to write from the input data.
            Optional. When not specified, the main output of the previous rule will be written.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True.
    """

    COMPRESSIONS = {
        'zip': '.zip',
        'gzip': '.gz',
        'bz2': '.bz2',
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
    """ Writes an existing dataframe to a parquet file on disk.

    The rule is a final rule, which means it produces no additional outputs, it takes any of the existing outputs and writes it to disk.

    Basic usage::

        # writes a file data.parquet and persists the main output of the previous rule to it
        rule = WriteParquetFileRule("data.parquet", "/home/myuser/")
        rule.apply(data)

        # writes a file test_data.parquet and persists the dataframe named input_data into it
        rule = WriteParquetFileRule("test_data.parquet", "/home/myuser/", named_input="input_data")
        rule.apply(data)

    Args:
        file_name: The name of the parquet file to write to disk. It will be written in the directory
            specified by the file_dir parameter.
        file_dir: The file directory where the file_name should be written.
            Defaults to . (ie the current directory).
        compression: Compress the parquet file using a supported compression algorithms. Optional.
            The following compression algorithms are supported: "snappy", "gzip", "brotli", "lz4", "zstd".

        named_input (Optional[str]): Select by name the dataframe to write from the input data.
            Optional. When not specified, the main output of the previous rule will be written.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True.
    """

    COMPRESSIONS = ("snappy", "gzip", "brotli", "lz4", "zstd")

    def __init__(self, file_name: str, file_dir: str=".", compression: Optional[str]=None, named_input: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
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
