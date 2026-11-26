# API Reference

## Plans and running plans

* [Plans, rule engine and data](core.md): `Plan`, `RuleEngine`, `RuleData`, `load_plan`, `run_plan`
* [Base classes for custom rules](custom-rules.md)
* [Exceptions](exceptions.md)

## Rules

The rules are available in each backend package: `etlrules.backends.pandas`, `etlrules.backends.polars`,
`etlrules.backends.dask` and `etlrules.backends.pyarrow`. Use the package of the backend you want to run the plan
with, e.g.:

```python
from etlrules.backends.pandas import ProjectRule, SortRule
```

When a plan is loaded from yaml, the backend is chosen when loading it (e.g. `load_plan("plan.yml", "polars")`).

* [Basic rules](basic.md)
* [Aggregation rules](aggregate.md)
* [Concatenation rules](concat.md)
* [Conditional rules](conditions.md)
* [Datetime rules](datetime.md)
* [Fill rules](fill.md)
* [Join rules](joins.md)
* [New column rules](newcolumns.md)
* [Numeric rules](numeric.md)
* [String rules](strings.md)
* [Type conversion rules](types.md)
* [File rules](io-files.md)
* [Database rules](io-db.md)

## Backend differences

### pyarrow

The pyarrow backend operates on `pyarrow.Table`s.

* Expressions (`AddNewColumnRule`, `IfThenElseRule`, `FilterRule`) support the python operators on columns:
  `+` (numbers and strings), `-`, `*`, `/`, `%`, `**`, comparisons, `&`, `|`, `^`, `~`, `<<` and `>>`,
  e.g. `df['A'] + df['B'] * 2`. Other expressions, e.g. using `and`, `or` or `if ... else`, are evaluated row by
  row (slower), with `df['A']` being the python value of the column in each row. pandas or polars specific
  methods (e.g. `df['A'].str.upper()`) are not supported.
* Regular expressions (`ReplaceRule` with `regex=True`, `StrExtractRule`) use the RE2 syntax, like the polars
  backend: look-around assertions and back references are not supported.
* `ReadSQLQueryRule` and `WriteSQLTableRule` use pandas (and SQLAlchemy) to read and write the data.
* `WriteCSVFileRule` quotes the column names and the string values.
