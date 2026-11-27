# History

## 0.5.0 (2026-11-27)

* Changes in behaviour to check when upgrading (they are bug fixes, but existing plans may rely on the old
  behaviour): `%f` in the formats of the polars backend is now the microseconds (like python, it was the
  nanoseconds); with the polars backend, `DateTimeDiffRule` rounds the components and `total_seconds` down, like the
  other backends (they were rounded towards zero for negative differences); reading files with `regex=True` raises
  FileNotFoundError when no files match; the dask backend writes the parquet files as `data_part_0.parquet`
  (instead of `data_part_0..parquet`, which are still read); `ReadCSVFileRule` in the pyarrow backend reads the
  dates and times as strings (like the other backends)
* Add `StrToDateTimeRule` to parse strings to datetime values with a given format (the reverse of
  `DateTimeToStrFormatRule`). In non-strict mode, the values which don't match the format become NA
* Add `FillNullRule` to replace the missing values in a set of columns with a given value
* Add `DropNullRowsRule` to remove the rows with missing values (in any or all of a set of columns)
* Add `CoalesceRule` to create a column with the first non-missing value from a list of columns
* Add `LimitRule` to keep only the first n rows (e.g. after a `SortRule` for the top n rows)
* Add `ClipRule` to limit the values of a numeric column to a lower and/or upper bound
* Add `StrSliceRule` to extract a substring from each value of a string column (like python's `value[start:end]`)
* Add `StrLengthRule` to calculate the length of the values in a string column
* Add `StrContainsRule` to check if the values in a string column contain, start with or end with a given
  string; the boolean result can be used to filter rows without backend specific expressions
* Add `SemiJoinRule` and `AntiJoinRule` to keep (or remove) the rows of a dataframe which have a match in another
  dataframe, e.g. to find the new records which have not been loaded yet
* Add `FilterValuesRule` to keep (or remove) the rows where a column has one of a list of values
* Add `AssertNotNullRule` and `AssertUniqueRule` to check the data (missing values, duplicate keys) and stop the
  plan with the new `ValidationError` when the check fails
* Add `ReadJSONFileRule` and `WriteJSONFileRule` to read and write JSON lines (NDJSON) files, optionally compressed
  (gzip, bz2, xz, zip). The values are read with the same (nullable) types in all the backends, the strings
  (including dates) are read as strings and `column_types` can set the types of the columns. The datetime values are
  written in the same ISO 8601 format in all the backends
* Fix reading multiple files with `regex=True` (`ReadCSVFileRule`, `ReadParquetFileRule`) in the pandas, polars
  and dask backends, which failed with NotImplementedError. The matching files are now read in the order of their
  names
* Fix `%f` (microseconds) in the formats of `DateTimeToStrFormatRule` and `StrToDateTimeRule` in the polars backend
  (it was interpreted as nanoseconds) and in the pyarrow backend (it wasn't supported)
* Fix `FilterRule` with `named_output_discarded` in graph mode plans: the rules using the discarded rows failed
  with a GraphRuntimeError (the named output wasn't declared)
* Fix `DateTimeDiffRule`: `total_seconds` failed in the pandas and dask backends when the difference had fractions
  of a second (it's now the whole seconds, rounded down, in all the backends); the components of negative
  differences were different in the polars backend; `nanoseconds` failed in the polars backend and `microseconds`
  was different in the pyarrow backend
* Fix `StrExtractRule` with named groups in the regular expression (e.g. `(?P<year>\d{4})`) in the pandas and dask
  backends
* Fix `AddNewColumnRule` with a constant expression: a string constant failed in the polars backend and a constant
  with a `column_type` failed in the pandas and dask backends
* Fix the names of the parquet files written by the dask backend (`data_part_0..parquet` had a double dot), which
  could not be read back with `regex=True`
* Reading files with `regex=True` raises FileNotFoundError when no files match (the output used to be None, which
  failed in the next rule)
* Fix `ReadCSVFileRule` in the pyarrow backend converting the values which look like dates, times or timestamps
  (the other backends read them as strings, which can be converted with `StrToDateTimeRule`)
* Fix the documentation of `TypeConversionRule`, which listed datetime and timedelta as supported types
  (use `StrToDateTimeRule` to convert strings to datetime values)

## 0.4.0 (2026-11-26)

* Add the pyarrow backend (`etlrules.backends.pyarrow`), where the rules operate on `pyarrow.Table`s. All the rules
  are supported, with the same behaviour as the other backends. It can also be used from the command line runner
  with `-b pyarrow`. See the "Backend differences" section of the API reference for the differences
  (expressions, regular expressions and SQL)
* The pyarrow backend doesn't need any extra dependencies: pyarrow (>= 14.0.1) is already a dependency of etlrules
* Update the README with the list of supported backends

## 0.3.6 (2026-11-26)

* Fix `AddNewColumnRule` with a `column_type` in the polars backend: with polars 2, a failed conversion raised
  polars' `InvalidOperationError` instead of the documented `TypeError`
* Fix `AddNewColumnRule` with a `column_type` in the dask backend: a failed conversion now raises `TypeError`
  as documented (dask evaluates lazily, so it is raised when the result is computed)
* Fix `ReadSQLQueryRule` in the dask backend returning different types than the pandas backend, e.g. integer
  columns with nulls were read as floats. The result now has the same nullable types (`Int64`, `string`,
  `Float64`, ...), including when a column only has nulls in some of the batches read
* Raise the minimum versions to the oldest versions tested: `pyarrow>=14.0.1` and `dask>=2025.5` for the
  dask backend. The minimum versions of all the dependencies are now tested in CI on Python 3.11
* Documentation: upgrade the docs to mkdocs 1.6, Material for MkDocs 9 and mkdocstrings 1, add an API reference
  page for each group of rules and publish the docs to https://ciprianmiclaus2.github.io/etlrules/ on each release
* Fix docstrings which didn't render correctly in the API reference
* Fix the tox environments (the test environments didn't install the polars and dask backends)

## 0.3.5 (2026-11-23)

* Support pandas 3 and polars 2: remove the `pandas<3` and `polars<1.32` pins. The minimum versions are now
  pandas 2.2.2 and polars 1.3
* Fix the test suite and the backends for pandas 3, polars 2, dask 2026.x and Python 3.14, which also fixes
  the dask known issues listed in 0.3.4
* Security: the dask `StrSplitRule` and `StrSplitRejoinRule` called `eval()` on the cell values when
  `ETLRULES_ENABLE_PYARROW_STRING` was set, so a crafted value in the input data could execute code.
  The values are now only split/joined as strings
* Remove the `ETLRULES_ENABLE_PYARROW_STRING` environment variable. Importing the dask backend no longer
  changes dask's global `dataframe.convert-string` setting (it used to disable it unless the variable was set).
  The dask backend now follows dask's own configuration: pyarrow backed strings by default, which can be
  disabled with dask's config (e.g. `DASK_DATAFRAME__CONVERT_STRING=False`). Both modes are tested
* Fix the dask backend with pyarrow backed strings: `list`/`csv` aggregations, and the row level fallback of
  expressions (e.g. adding an int and a string column) with pandas 2.x
* Fix `ReplaceRule` with `regex=True` in the pandas and dask backends, which only applied the first of the
  values/new_values pairs
* Fix `ReplaceRule` with `regex=True` in the polars backend, which only replaced the first match in each value
* Fix `WriteCSVFileRule` in the polars backend writing zip archives for the gzip, bz2 and xz compressions:
  the files are now written in the format matching their extension, readable by other tools and backends.
  `ReadCSVFileRule` still reads the zip archives with a .gz/.bz2/.xz extension written by earlier versions
* `run_plan` no longer parses the command line arguments (`sys.argv`), which made it exit when called from
  a program. It takes a new optional `context` argument to override the plan's context values
* Fix boolean plan context values given on the command line (e.g. `--my_flag false`)

## 0.3.4 (2026-11-26)

* Fix packaging: the 0.3.3 wheel did not include the `etlrules.backends` subpackages, so no backend could be imported
  from a PyPI install
* Require Python >= 3.11 and add support for Python 3.13 and 3.14
* Pin `pandas<3` and `polars<1.32`; newer major/minor versions are not supported yet
* Add an `etlrules` console script (equivalent to `python -m etlrules.runner`)
* Fix `etlrules.__version__`, which still reported 0.3.2; it is now the single source of the package version
* Known issue: the dask backend has a few failing scenarios with current dask/numpy/pyarrow releases
  (type conversion dtypes, row-level expression fallback, dedupe with keep='none', missing parquet columns)

## 0.3.3 (2024-10-31)

* Upgrade polars support to 1.9
* Fix perf_logger missing in pandas expressions

## 0.3.2 (2024-01-08)

* Add option to skip rows from the top of a csv file for the csv read rule
* Add support for reading/writing compressed csv files for the polars backend
* Support reading csv files via http and https
* Read csv files in one block in dask

## 0.3.1 (2023-12-22)

* Remove polars-business dependency and implement vectorized datetime operations for weekdays offsets
* Add tests to run the examples to make sure they don't get broken by future changes
* Optimize pandas business_offset and date_offset using values from a different columns to be vectorized operations
* Add a perf logger which will log a warning when using operations which are not vectorized

## 0.3.0 (2023-12-11)

* Add support for dask backend
* Ability to deserialize custom rules (ie not part of the etlrules package) to support users implementing their own rules

## 0.2.3 (2023-11-28)

* Fix to apply substitution in the WriteSQLTableRule sql_engine parameter
* Apply substitution in the Read/Write rules for csv and parquet files for the file_name and file_dir parameters
* Add a cli runner which allows users to run a yml file and parameterize with cli args the plan context
* Add the csv2db plan/yml example
* Add the db2csv plan/yml example
* Remove poetry

## 0.2.2 (2023-11-26)

* Support environment variables substitution in the sql_engine string for SQL rules
* Add support for the Boolean type
* Introduce the concept of a plan context, consisting of a key-value mapping of string to int/float/str/bool values
  which will act as the args into the plan. They can be used in expressions when adding new columns, ifthenelse and
  filter rules.
* Add env and context substitution feature to sql queries

## 0.2.1 (2023-11-20)

* Add support for unsigned int types
* Add AddRowNumbersRule, a rule to add row numbers
* Add ExplodeValuesRule which explode lists of values into individual scalar values as additional rows
* Add support for reading/writing to/from DBs via sqlalchemy under etlrules.backends.common.io.db

## 0.2.0 (2023-11-14)

* Add support for polars as a backend

## 0.1.1 (2023-10-29)

* Fix bug in RuleEngine validate affecting pipeline mode
* Export the main classes from etlrules __init__
* More typing annotations
* More documentation

## 0.1.0 (2023-10-28)

* First release on PyPI.
* Rule engine supporting running pipeline mode and graph mode plans
* Support for an initial set of etl rules, pandas backend only
