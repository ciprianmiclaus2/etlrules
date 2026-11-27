import os, re
from typing import List, Mapping, NoReturn, Optional, Sequence, Tuple, Union

from etlrules.rule import BaseRule, UnaryOpBaseRule
from etlrules.backends.common.substitution import subst_string
from etlrules.backends.common.types import SUPPORTED_TYPES
from etlrules.exceptions import MissingColumnError, UnsupportedTypeError


class BaseReadFileRule(BaseRule):
    def __init__(self, file_name: str, file_dir: Optional[str]=None, regex: bool=False, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_output=named_output, name=name, description=description, strict=strict)
        self.file_name = file_name
        self.file_dir = file_dir
        self.regex = bool(regex)
        if self._is_uri() and self.regex:
            raise ValueError("Regex read not supported for URIs.")

    def _is_uri(self):
        file_name = self.file_name.lower()
        return file_name.startswith("http://") or file_name.startswith("https://")

    def has_input(self):
        return False

    def _get_full_file_paths(self):
        file_name = subst_string(self.file_name)
        file_dir = subst_string(self.file_dir or "")
        if self.regex:
            pattern = re.compile(file_name)
            for fn in sorted(os.listdir(file_dir)):  # sorted: a deterministic order of the rows
                if pattern.match(fn):
                    yield os.path.join(file_dir, fn)
        else:
            if self._is_uri():
                yield file_name
            else:
                yield os.path.join(file_dir, file_name)

    def do_read(self, file_path: str):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def do_concat(self, left_df, right_df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)

        result = None
        for file_path in self._get_full_file_paths():
            df = self.do_read(file_path)
            if result is None:
                result = df
            else:
                result = self.do_concat(result, df)
        self._set_output_df(data, result)


class ReadCSVFileRule(BaseReadFileRule):
    r""" Reads one or multiple csv files from a directory and persists it as a dataframe for subsequent rules to operate on.

    Basic usage:

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
            It can also be an URI (e.g. https://example.com/mycsv.csv)
        file_dir: The file directory where the file_name is located. When file_name is a regular expression and 
            the regex parameter is True, file_dir is the directory that is inspected for any files that match the
            regular expression. Optional.
            For files it defaults to . (ie the current directory). Ignored for URIs.
        regex: When True, the file_name is interpreted as a regular expression. Defaults to False.
        separator: The single character to be used as separator in the csv file. Defaults to , (comma).
        header: When True, the first line is interpreted as the header and the column names are extracted from it.
            When False, the first line is part of the data and the columns will have names like 0, 1, 2, etc.
            Defaults to True.
        skip_header_rows: Optional number of rows to skip at the top of the file, before the header.

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

    def __init__(self, file_name: str, file_dir: Optional[str]=None, regex: bool=False, separator: str=",",
                 header: bool=True, skip_header_rows: Optional[int]=None,
                 named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(file_name=file_name, file_dir=file_dir, regex=regex, named_output=named_output, name=name, description=description, strict=strict)
        self.separator = separator
        self.header = header
        self.skip_header_rows = skip_header_rows


class ReadParquetFileRule(BaseReadFileRule):
    r""" Reads one or multiple parquet files from a directory and persists it as a dataframe for subsequent rules to operate on.

    Basic usage:

        # reads a file data.csv and persists it as the main output of the rule
        rule = ReadParquetFileRule("data.parquet", "/home/myuser/")
        rule.apply(data)

        # reads a file test_data.parquet and persists it as the input_data named output
        # other rules can specify input_data as their named_input to operate on it
        rule = ReadParquetFileRule("test_data.parquet", "/home/myuser/", named_output="input_data")
        rule.apply(data)

        # reads all the files with the .parquet extension from the home dir of myuser and
        # concatenates them into a single dataframe
        rule = ReadParquetFileRule(".*\.parquet", "/home/myuser/", named_output="input_data")
        rule.apply(data)

        # reads only the A,B,C columns from the file data.csv file
        rule = ReadParquetFileRule("data.parquet", "/home/myuser/", columns=["A", "B", "C"])
        rule.apply(data)

        # reads only those rows where column A is greater than 10 and column B is True
        rule = ReadParquetFileRule("data.parquet", "/home/myuser/", filters=[["A", ">=", 10], ["B", "==", True]])
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
        self.filters = self._get_filters(filters) if filters is not None else None

    def _raise_filters_invalid(self, error: str) -> NoReturn:
        raise ValueError(f"Invalid filters. It must be a List[Tuple] or List[List[Tuple]] with each Tuple being (column, op, value): {error}")

    def _validate_tuple(self, tpl):
        if len(tpl) != 3 or not isinstance(tpl[0], str) or not isinstance(tpl[1], str):
            self._raise_filters_invalid(f"Third level expected a list/tuple (cond, op, value), got: {tpl}.")
        op = tpl[1]
        if op not in self.SUPPORTED_FILTERS_OPS:
            self._raise_filters_invalid(f"Invalid operator {op} in {tpl}. Must be one of: {self.SUPPORTED_FILTERS_OPS}.")
        value = tpl[2]
        if op in ("in", "not in"):
            if not isinstance(value, (list, tuple, set)):
                self._raise_filters_invalid(f"Invalid value type for {value} for {op} operand in {tpl}. Must be list/tuple/set.")
            else:
                value = list(value)
        return (tpl[0], op, value)

    def _get_filters(self, filters):
        lst = []
        if isinstance(filters, (list, tuple)):
            if not filters:
                return None
            for filter2 in filters:
                if isinstance(filter2, (list, tuple)) and filter2:
                    if len(filter2) == 3 and isinstance(filter2[0], str):
                        # List[Tuple] form
                        tpl = self._validate_tuple(filter2)
                        lst.append(tpl)
                    else:
                        lst2 = []
                        for filter3 in filter2:
                            if isinstance(filter3, (list, tuple)) and filter3:
                                tpl = self._validate_tuple(filter3)
                                lst2.append(tpl)
                            else:
                                self._raise_filters_invalid(f"Third level expected a list/tuple, got: {filter3}.")
                        lst.append(lst2)
                else:
                    self._raise_filters_invalid(f"Second level expected a list/tuple, got: {filter2}.")
        else:
           self._raise_filters_invalid(f"Top level expected a list/tuple, got: {filters}")
        return lst


class ReadJSONFileRule(BaseReadFileRule):
    r""" Reads one or multiple JSON lines files and persists them as a dataframe for subsequent rules to operate on.

    The files must be in the JSON lines format (also known as NDJSON): one JSON object per line, each object being
    a row with the columns as keys. For example:

        {"Id": 1, "Name": "Mike", "Joined": "2023-05-19"}
        {"Id": 2, "Name": "Anna"}

    Missing keys (and null values) are missing values (NA) in the result.

    Basic usage:

        # reads a file data.json and persists it as the main output of the rule
        rule = ReadJSONFileRule("data.json", "/home/myuser/")
        rule.apply(data)

        # reads all the files events_YYYYMMDD.json.gz in a directory and concatenates them
        rule = ReadJSONFileRule(r"events_\d{8}\.json\.gz", "/home/myuser/", regex=True, named_output="events")
        rule.apply(data)

        # reads a file, converting the Id column to int32 (rather than relying on the type inference)
        rule = ReadJSONFileRule("data.json", "/home/myuser/", column_types={"Id": "int32", "Name": "string"})
        rule.apply(data)

    Args:
        file_name: The name of the JSON lines file to load. The extensions .gz, .bz2, .xz and .zip indicate a
            compressed file (.zip archives must contain a single file).
            file_name can also be a regular expression (specify regex=True in that case).
            The reader will find all the files in the file_dir directory that match the regular expression and extract
            all those files and concatenate them into a single dataframe.
            It can also be an URI (e.g. https://example.com/data.json)
        file_dir: The file directory where the file_name is located. When file_name is a regular expression and
            the regex parameter is True, file_dir is the directory that is inspected for any files that match the
            regular expression. Optional.
            For files it defaults to . (ie the current directory). Ignored for URIs.
        regex: When True, the file_name is interpreted as a regular expression. Defaults to False.
        column_types: A mapping of column names and their types. Optional.
            The types are inferred from the data, but the inference can differ between backends (and the type of
            a column which only has missing values can't be inferred): specifying the types makes the result the same
            in all the backends. The supported types are: int8, int16, int32, int64, uint8, uint16,
            uint32, uint64, float32, float64, string and boolean.

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter validation. Default: True

    Raises:
        IOError: raised when the file is not found.
        UnsupportedTypeError: raised if any of the column_types are not supported.
        MissingColumnError: raised in strict mode only if a column in column_types is not in the data.
            In non-strict mode, the missing columns are ignored.

    Note:
        The values which are JSON strings are read as strings, including dates and times: use StrToDateTimeRule to
        convert them to datetime values.

    Note:
        Nested JSON objects and arrays are read as each backend represents them: python dicts and lists with the pandas
        and dask backends, structs and lists with the polars and pyarrow backends.
    """

    def __init__(self, file_name: str, file_dir: Optional[str]=None, regex: bool=False, column_types: Optional[Mapping[str, str]]=None,
                 named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(file_name=file_name, file_dir=file_dir, regex=regex, named_output=named_output, name=name, description=description, strict=strict)
        self.column_types = dict(column_types) if column_types is not None else None
        for column, column_type in (self.column_types or {}).items():
            if column_type not in SUPPORTED_TYPES:
                raise UnsupportedTypeError(f"Type '{column_type}' for column '{column}' is not supported.")

    def _get_column_types(self, df_columns):
        """ The column_types for the columns in the data (raises MissingColumnError in strict mode for the other columns). """
        if not self.column_types:
            return {}
        missing = set(self.column_types) - set(df_columns)
        if missing and self.strict:
            raise MissingColumnError(f"Column(s) {missing} in column_types are missing from the data.")
        return {col: col_type for col, col_type in self.column_types.items() if col not in missing}


class BaseWriteFileRule(UnaryOpBaseRule):

    EXCLUDE_FROM_SERIALIZE = ("named_output", )

    def __init__(self, file_name, file_dir=".", named_input=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=None, name=name, description=description, strict=strict)
        self.file_name = file_name
        self.file_dir = file_dir

    def has_output(self):
        return False

    def do_write(self, file_name: str, file_dir: str, df) -> None:
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        self.do_write(subst_string(self.file_name), subst_string(self.file_dir), df)


class WriteCSVFileRule(BaseWriteFileRule):
    """ Writes an existing dataframe to a csv file (optionally compressed) on disk.

    The rule is a final rule, which means it produces no additional outputs, it takes any of the existing outputs and writes it to disk.

    Basic usage:

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

    def __init__(self, file_name: str, file_dir: str=".", separator: str=",", header: bool=True, compression: Optional[str]=None, named_input: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(
            file_name=file_name, file_dir=file_dir, named_input=named_input, 
            name=name, description=description, strict=strict)
        self.separator = separator
        self.header = header
        assert compression is None or compression in self.COMPRESSIONS.keys(), f"Unsupported compression '{compression}'. It must be one of: {self.COMPRESSIONS.keys()}."
        if compression:
            assert file_name.endswith(self.COMPRESSIONS[compression]), f"The file name {file_name} must have the extension {self.COMPRESSIONS[compression]} when the compression is set to {compression}."
        self.compression = compression


class WriteParquetFileRule(BaseWriteFileRule):
    """ Writes an existing dataframe to a parquet file on disk.

    The rule is a final rule, which means it produces no additional outputs, it takes any of the existing outputs and writes it to disk.

    Basic usage:

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


class WriteJSONFileRule(BaseWriteFileRule):
    """ Writes an existing dataframe to a JSON lines file (optionally compressed) on disk.

    Each row is written as a JSON object on its own line (also known as NDJSON), with the columns as keys.
    The rule is a final rule, which means it produces no additional outputs, it takes any of the existing outputs and writes it to disk.

    Basic usage:

        # writes a file data.json and persists the main output of the previous rule to it
        rule = WriteJSONFileRule("data.json", "/home/myuser/")
        rule.apply(data)

        # writes a gzip compressed file with the dataframe named input_data
        rule = WriteJSONFileRule("data.json.gz", "/home/myuser/", compression="gzip", named_input="input_data")
        rule.apply(data)

    Args:
        file_name: The name of the JSON lines file to write to disk. It will be written in the directory
            specified by the file_dir parameter.
        file_dir: The file directory where the file_name should be written.
            Defaults to . (ie the current directory).
        compression: Compress the file using a supported compression algorithm. Optional.
            When the compression is specified, the file_name must end with the extension associated with that
            compression format. The following options are supported:
            zip - file_name must end with .zip (e.g. output.json.zip), will produce a zipped file
            gzip - file_name must end with .gz (e.g. output.json.gz), will produce a gzipped file
            bz2 - file_name must end with .bz2 (e.g. output.json.bz2), will produce a bzipped file
            xz - file_name must end with .xz (e.g. output.json.xz), will produce a xz-compressed file

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter validation. Default: True

    Note:
        Missing values are written as null. Datetime values are written as ISO 8601 strings with microseconds
        (e.g. 2023-05-19T10:30:45.000000) in all the backends. The files are written in UTF-8.
    """

    COMPRESSIONS = {
        'zip': '.zip',
        'gzip': '.gz',
        'bz2': '.bz2',
        'xz': '.xz',
    }

    # the format used to write the datetime values
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

    def __init__(self, file_name: str, file_dir: str=".", compression: Optional[str]=None, named_input: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(
            file_name=file_name, file_dir=file_dir, named_input=named_input,
            name=name, description=description, strict=strict)
        assert compression is None or compression in self.COMPRESSIONS.keys(), f"Unsupported compression '{compression}'. It must be one of: {self.COMPRESSIONS.keys()}."
        if compression:
            assert file_name.endswith(self.COMPRESSIONS[compression]), f"The file name {file_name} must have the extension {self.COMPRESSIONS[compression]} when the compression is set to {compression}."
        self.compression = compression
