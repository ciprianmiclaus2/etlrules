# API Reference

## Plans and running plans

* [Plans, rule engine and data](core.md): `Plan`, `RuleEngine`, `RuleData`, `load_plan`, `run_plan`
* [Base classes for custom rules](custom-rules.md)
* [Exceptions](exceptions.md)

## Rules

The rules are available in each backend package: `etlrules.backends.pandas`, `etlrules.backends.polars`
and `etlrules.backends.dask`. Use the package of the backend you want to run the plan with, e.g.:

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
