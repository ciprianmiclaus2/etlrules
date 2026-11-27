# Conditional rules

Rules to filter rows and calculate columns based on conditions.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import IfThenElseRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.conditions.IfThenElseRule
    options:
      members: false

::: etlrules.backends.common.conditions.FilterRule
    options:
      members: false

::: etlrules.backends.common.conditions.FilterValuesRule
    options:
      members: false
