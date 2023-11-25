# History

## 0.2.2 (XXXX-XX-XX)

* Support environment variables substitution in the sql_engine string for SQL rules
* Add support for the Boolean type

## 0.2.1 (2023-11-20)

* Add support for unsigned int types
* Add AddRowNumbersRule, a rule to add row numbers
* Add ExplodeValuesRule which explode lists of values into individual scalar values as additional rows
* Add support for reading/writing to/from DBs via sqlalchemy under etlrules.backends.common.io.db

## 0.2.0 (2023-11-14)

* Add support for polars as a backend

## 0.1.1 (2023-10-29)

* Fix bug in RuleEngine validate affecting pipeline mode
* Export the main classes from etlrules __init__
* More typing annotations
* More documentation

## 0.1.0 (2023-10-28)

* First release on PyPI.
* Rule engine supporting running pipeline mode and graph mode plans
* Support for an initial set of etl rules, pandas backend only
