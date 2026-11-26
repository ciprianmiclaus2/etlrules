# String rules

Rules operating on string columns.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import StrLowerRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.strings.StrLowerRule
    options:
      members: false

::: etlrules.backends.common.strings.StrUpperRule
    options:
      members: false

::: etlrules.backends.common.strings.StrCapitalizeRule
    options:
      members: false

::: etlrules.backends.common.strings.StrSplitRule
    options:
      members: false

::: etlrules.backends.common.strings.StrSplitRejoinRule
    options:
      members: false

::: etlrules.backends.common.strings.StrStripRule
    options:
      members: false

::: etlrules.backends.common.strings.StrPadRule
    options:
      members: false

::: etlrules.backends.common.strings.StrExtractRule
    options:
      members: false
