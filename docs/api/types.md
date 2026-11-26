# Type conversion rules

Rules to convert the types of columns.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import TypeConversionRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.types.TypeConversionRule
    options:
      members: false
