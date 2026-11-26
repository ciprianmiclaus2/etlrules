# Basic rules

Rules to dedupe, project, rename, replace values, sort and explode, and to group rules into blocks.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import DedupeRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.basic.DedupeRule
    options:
      members: false

::: etlrules.backends.common.basic.ProjectRule
    options:
      members: false

::: etlrules.backends.common.basic.RenameRule
    options:
      members: false

::: etlrules.backends.common.basic.ReplaceRule
    options:
      members: false

::: etlrules.backends.common.basic.SortRule
    options:
      members: false

::: etlrules.backends.common.basic.ExplodeValuesRule
    options:
      members: false

::: etlrules.backends.common.basic.RulesBlock
    options:
      members: false
