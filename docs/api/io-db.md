# Database rules

Rules to read (extract) dataframes from SQL databases and write (load) them to SQL tables.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import ReadSQLQueryRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.io.db.ReadSQLQueryRule
    options:
      members: false

::: etlrules.backends.common.io.db.WriteSQLTableRule
    options:
      members: false
