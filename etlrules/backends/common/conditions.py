from typing import Optional, Union

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.rule import UnaryOpBaseRule


class IfThenElseRule(UnaryOpBaseRule):
    """ Calculates the ouput based on a condition (If Cond is true Then use then_value Else use else_value).

    Example::

        Given df:
        | A   | B  |
        | 1   | 2  |
        | 5   | 3  |
        | 3   | 4  |

        rule = IfThenElseRule("df['A'] > df['B']", output_column="C", then_value="A is greater", else_value="B is greater")
        rule.apply(df)

    Result::

        | A   | B  | C            |
        | 1   | 2  | B is greater |
        | 5   | 3  | A is greater |
        | 3   | 4  | B is greater |

    Args:
        condition_expression: An expression as a string. The expression must evaluate to a boolean scalar or a boolean series.
        then_value: The value to use if the condition is true.
        then_column: Use the value from the then_column if the condition is true.
            One and only one of then_value and then_column can be used.
        else_value: The value to use if the condition is false.
        else_column: Use the value from the else_column if the condition is false.
            One and only one of the else_value and else_column can be used.
        output_column: The column name of the result column which will be added to the dataframe.

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
        ColumnAlreadyExistsError: raised in strict mode only if a column with the same name already exists in the dataframe.
        ExpressionSyntaxError: raised if the column expression has a Python syntax error.
        MissingColumnError: raised when then_column or else_column are used but they are missing from the input dataframe.
        TypeError: raised if an operation is not supported between the types involved
        NameError: raised if an unknown variable is used
        KeyError: raised if you try to use an unknown column (i.e. df['ANY_UNKNOWN_COLUMN'])
    """

    EXCLUDE_FROM_COMPARE = ('_condition_expression', )

    def __init__(self, condition_expression: str, output_column: str, then_value: Optional[Union[int,float,bool,str]]=None, then_column: Optional[str]=None,
                 else_value: Optional[Union[int,float,bool,str]]=None, else_column: Optional[str]=None, 
                 named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert bool(then_value is None) != bool(then_column is None), "One and only one of then_value and then_column can be specified."
        assert bool(else_value is None) != bool(else_column is None), "One and only one of else_value and else_column can be specified."
        assert condition_expression, "condition_expression cannot be empty"
        assert output_column, "output_column cannot be empty"
        self.condition_expression = condition_expression
        self.output_column = output_column
        self.then_value = then_value
        self.then_column = then_column
        self.else_value = else_value
        self.else_column = else_column
        self._condition_expression = self.get_condition_expression()

    def _validate_columns(self, df_columns):
        if self.strict and self.output_column in df_columns:
            raise ColumnAlreadyExistsError(f"Column {self.output_column} already exists in the input dataframe.")
        if self.then_column is not None and self.then_column not in df_columns:
            raise MissingColumnError(f"Column {self.then_column} is missing from the input dataframe.")
        if self.else_column is not None and self.else_column not in df_columns:
            raise MissingColumnError(f"Column {self.else_column} is missing from the input dataframe.")


class FilterRule(UnaryOpBaseRule):
    """ Exclude rows based on a condition.

    Example::

        Given df:
        | A   | B  |
        | 1   | 2  |
        | 5   | 3  |
        | 3   | 4  |

        rule = FilterRule("df['A'] > df['B']")
        rule.apply(df)

    Result::

        | A   | B  |
        | 5   | 3  |

    Same example using discarded_matching_rows=True::

        rule = FilterRule("df['A'] > df['B']", discard_matching_rows=True)
        rule.apply(df)

    Result::

        | A   | B  |
        | 1   | 2  |
        | 3   | 4  |

    Args:
        condition_expression: An expression as a string. The expression must evaluate to a boolean scalar or a boolean series.
        discard_matching_rows: By default the rows matching the condition (ie where the condition is True) are kept, the rest of the
            rows being dropped from the result. Setting this parameter to True essentially inverts the condition, so the rows
            matching the condition are discarded and the rest of the rows kept. Default: False.
        named_output_discarded: A named output for the records being discarded if those need to be kept for further processing.
            Default: None, which doesn't keep track of discarded records.

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
        ExpressionSyntaxError: raised if the column expression has a Python syntax error.
        TypeError: raised if an operation is not supported between the types involved
        NameError: raised if an unknown variable is used
        KeyError: raised if you try to use an unknown column (i.e. df['ANY_UNKNOWN_COLUMN'])
    """

    EXCLUDE_FROM_COMPARE = ('_condition_expression', )

    def __init__(self, condition_expression: str, discard_matching_rows: bool=False, named_output_discarded: Optional[str]=None,
                 named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert condition_expression, "condition_expression cannot be empty"
        self.condition_expression = condition_expression
        self.discard_matching_rows = discard_matching_rows
        self.named_output_discarded = named_output_discarded
        self._condition_expression = self.get_condition_expression()
