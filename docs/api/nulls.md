# Missing values rules

Rules to replace missing values (NA/nulls) with a given value, drop the rows with missing values and take the first
non-missing value from a list of columns. To fill missing values with values from other rows, see the
[fill rules](fill.md).

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import FillNullRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.nulls.FillNullRule
    options:
      members: false

::: etlrules.backends.common.nulls.DropNullRowsRule
    options:
      members: false

::: etlrules.backends.common.nulls.CoalesceRule
    options:
      members: false
