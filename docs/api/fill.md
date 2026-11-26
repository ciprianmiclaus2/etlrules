# Fill rules

Rules to fill missing values (NA) with values from other rows.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import ForwardFillRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.fill.ForwardFillRule
    options:
      members: false

::: etlrules.backends.common.fill.BackFillRule
    options:
      members: false
