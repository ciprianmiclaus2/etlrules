# Numeric rules

Rules operating on numeric columns.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import RoundRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.numeric.RoundRule
    options:
      members: false

::: etlrules.backends.common.numeric.AbsRule
    options:
      members: false
