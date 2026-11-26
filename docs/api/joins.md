# Join rules

Database-style joins of two dataframes.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import LeftJoinRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.joins.LeftJoinRule
    options:
      members: false

::: etlrules.backends.common.joins.InnerJoinRule
    options:
      members: false

::: etlrules.backends.common.joins.OuterJoinRule
    options:
      members: false

::: etlrules.backends.common.joins.RightJoinRule
    options:
      members: false
