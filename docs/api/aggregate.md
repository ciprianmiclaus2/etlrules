# Aggregation rules

SQL-like group by and aggregation.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import AggregateRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.aggregate.AggregateRule
    options:
      members: false
