# Datetime rules

Rules operating on datetime columns.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import DateTimeRoundRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.datetime.DateTimeRoundRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeRoundDownRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeRoundUpRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeExtractComponentRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeAddRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeSubstractRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeDiffRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeUTCNowRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeLocalNowRule
    options:
      members: false

::: etlrules.backends.common.datetime.DateTimeToStrFormatRule
    options:
      members: false
