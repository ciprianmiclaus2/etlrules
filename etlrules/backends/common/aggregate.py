import ast
from typing import Iterable, Mapping, Optional

from pandas import isnull

from etlrules.exceptions import (
    ColumnAlreadyExistsError,
    ExpressionSyntaxError,
    MissingColumnError,
    UnsupportedTypeError,
)
from etlrules.backends.common.types import SUPPORTED_TYPES
from etlrules.rule import UnaryOpBaseRule


class AggregateRule(UnaryOpBaseRule):
    """Performs a SQL-like groupby and aggregation.

    It takes a list of columns to group by and the result will have one row for each unique combination
    of values in the group_by columns.
    The rest of the columns (not in the group_by) can be aggregated using either pre-defined aggregations
    or using custom python expressions.

    Args:
        group_by: A list of columns to group the result by
        aggregations: A mapping {column_name: aggregation_function} which specifies how to aggregate
            columns which are not in the group_by list.
            The following list of aggregation functions are supported::

                min: minimum of the values in the group
                max: minimum of the values in the group
                mean: The mathematical mean value in the group
                count: How many values are in the group, including NA
                countNoNA: How many values are in the group, excluding NA
                sum: The sum of the values in the group
                first: The first value in the group
                last: The last value in the group
                list: Produces a python list with all the values in the group, excluding NA
                tuple: Like list above but produces a tuple
                csv: Produces a comma separated string of values, exluding NA

        aggregation_expressions: A mapping {column_name: aggregation_expression} which specifies how to aggregate
            columns which are not in the group_by list.
            The aggregation expression is a string representing a valid Python expression which gets evaluated.
            The input will be in a variable `values`. `isnull` can be used to filter out NA.

            Example::

                {"C": "';'.join(str(v) for v in values if not isnull(v))"}

                The above aggregates the column C by producing a ; separated string of values in the group, excluding NA.

        aggregation_types: An optional mapping of {column_name: column_type} which converts the respective output
            column to the given type. The supported types are: int8, int16, int32, int64, uint8, uint16,
            uint32, uint64, float32, float64, string, boolean, datetime and timedelta.

        named_input: Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output: Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name: Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict: When set to True, the rule does a stricter valiation. Default: True

    Raises:
        ColumnAlreadyExistsError: raised if a column appears in multiple places in group_by/aggregations/aggregation_expressions.
        ExpressionSyntaxError: raised if any aggregation expression (if any are passed in) has a Python syntax error.
        MissingColumnError: raised in strict mode only if a column specified in aggregations or aggregation_expressions
            is missing from the input dataframe. If aggregation_types are specified, it is raised in strict mode if a column
            in the aggregation_types is missing from the input dataframe.
        UnsupportedTypeError: raised if a type specified in aggregation_types is not supported.
        ValueError: raised if a column in aggregations is trying to be aggregated using an unknown aggregate function
        TypeError: raised if an operation is not supported between the types involved
        NameError: raised if an unknown variable is used

    Note:
        Other Python exceptions can be raised when custom aggregation expressions are used, depending on what the expression is doing.

    Note:
        Any columns not in the group_by list and not present in either aggregations or aggregation_expressions will be dropped from the result.
    """

    AGGREGATIONS = {}

    EXCLUDE_FROM_COMPARE = ("_aggs",)

    def __init__(
        self,
        group_by: Iterable[str],
        aggregations: Optional[Mapping[str, str]] = None,
        aggregation_expressions: Optional[Mapping[str, str]] = None,
        aggregation_types: Optional[Mapping[str, str]] = None,
        named_input: Optional[str] = None,
        named_output: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        strict: bool = True,
    ):
        super().__init__(
            named_input=named_input, named_output=named_output, name=name,
            description=description, strict=strict
        )
        self.group_by = [col for col in group_by]
        assert aggregations or aggregation_expressions, "One of aggregations or aggregation_expressions must be specified."
        if aggregations is not None:
            self.aggregations = {}
            for col, agg_func in aggregations.items():
                if col in self.group_by:
                    raise ColumnAlreadyExistsError(f"Column {col} appears in group_by and cannot be aggregated.")
                if agg_func not in self.AGGREGATIONS:
                    raise ValueError(f"'{agg_func}' is not a supported aggregation function.")
                self.aggregations[col] = agg_func
        else:
            self.aggregations = None
        self._aggs = {}
        if self.aggregations:
            self._aggs.update({
                key: self.AGGREGATIONS[agg_func]
                for key, agg_func in (aggregations or {}).items()
            })
        if aggregation_expressions is not None:
            self.aggregation_expressions = {}
            for col, agg_expr in aggregation_expressions.items():
                if col in self.group_by:
                    raise ColumnAlreadyExistsError(f"Column {col} appears in group_by and cannot be aggregated.")
                if col in self._aggs:
                    raise ColumnAlreadyExistsError(f"Column {col} is already being aggregated.")
                try:
                    _ast_expr = ast.parse(agg_expr, filename=f"{col}_expression.py", mode="eval")
                    _compiled_expr = compile(_ast_expr, filename=f"{col}_expression.py", mode="eval")
                    self._aggs[col] = lambda values, bound_compiled_expr=_compiled_expr: eval(
                        bound_compiled_expr, {"isnull": isnull}, {"values": values}
                    )
                except SyntaxError as exc:
                    raise ExpressionSyntaxError(f"Error in aggregation expression for column '{col}': '{agg_expr}': {str(exc)}")
                self.aggregation_expressions[col] = agg_expr

        if aggregation_types is not None:
            self.aggregation_types = {}
            for col, col_type in aggregation_types.items():
                if col not in self._aggs and col not in self.group_by:
                    if self.strict:
                        raise MissingColumnError(f"Column {col} is neither in the group by columns nor in the aggregations.")
                    else:
                        continue
                if col_type not in SUPPORTED_TYPES:
                    raise UnsupportedTypeError(f"Unsupported type '{col_type}' for column '{col}'.")
                self.aggregation_types[col] = col_type
        else:
            self.aggregation_types = None

    def do_aggregate(self, df, aggs):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df_columns_set = set(df.columns)
        if not set(self._aggs) <= df_columns_set:
            if self.strict:
                raise MissingColumnError(f"Missimg columns to aggregate by: {set(self._aggs) - df_columns_set}.")
            aggs = {
                col: agg for col, agg in self._aggs.items() if col in df_columns_set
            }
        else:
            aggs = self._aggs
        df = self.do_aggregate(df, aggs)
        self._set_output_df(data, df)
