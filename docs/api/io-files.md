# File rules

Rules to read (extract) dataframes from files and write (load) them to files.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import ReadCSVFileRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.io.files.ReadCSVFileRule
    options:
      members: false

::: etlrules.backends.common.io.files.ReadParquetFileRule
    options:
      members: false

::: etlrules.backends.common.io.files.WriteCSVFileRule
    options:
      members: false

::: etlrules.backends.common.io.files.WriteParquetFileRule
    options:
      members: false
