# Validation rules

Rules which check the data and stop the plan with a `ValidationError` when the check fails (e.g. missing values or
duplicate keys before loading the data into a database). The data is not changed: the output of a validation rule
is its input.

!!! note "Importing the rules"
    The rules are implemented by each backend. Import them from the package of the backend you use, e.g.
    `from etlrules.backends.pandas import AssertNotNullRule` (or `etlrules.backends.polars`, `etlrules.backends.dask`, `etlrules.backends.pyarrow`).
    The arguments and the behaviour are the same in all the backends, unless stated otherwise.

::: etlrules.backends.common.validation.AssertNotNullRule
    options:
      members: false

::: etlrules.backends.common.validation.AssertUniqueRule
    options:
      members: false
