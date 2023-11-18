from typing import Optional

from etlrules.exceptions import ColumnAlreadyExistsError, UnsupportedTypeError
from etlrules.rule import UnaryOpBaseRule
from etlrules.backends.common.types import SUPPORTED_TYPES


class AddNewColumnRule(UnaryOpBaseRule):
    """ Adds a new column and sets it to the value of an evaluated expression.

    Example::

        Given df:
        | A   | B  |
        | 1   | 2  |
        | 2   | 3  |
        | 3   | 4  |

    > AddNewColumnRule("Sum", "df['A'] + df['B']").apply(df)

    Result::

        | A   | B  | Sum |
        | 1   | 2  | 3   |
        | 2   | 3  | 5   |
        | 3   | 4  | 7   |

    Args:
        output_column: The name of the new column to be added.
        column_expression: An expression that gets evaluated and produces the value for the new column.
            The syntax: df["EXISTING_COL"] can be used in the expression to refer to other columns in the dataframe.
        column_type: An optional type to convert the result to. If not specified, the type is determined from the
            output of the expression, which can sometimes differ based on the backend.
            If the input dataframe is empty, this type ensures the column will be of the specified type, rather than
            default to string type.

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
        UnsupportedTypeError: raised if the column_type parameter is specified and not supported.
        TypeError: raised if an operation is not supported between the types involved. raised when the column type is specified
            but the conversion to that type fails.
        NameError: raised if an unknown variable is used
        KeyError: raised if you try to use an unknown column (i.e. df['ANY_UNKNOWN_COLUMN'])

    Note:
        The implementation will try to use dataframe operations for performance, but when those are not supported it
        will fallback to row level operations.
    
    Note:
        NA are treated slightly differently between dataframe level operations and row level.
        At dataframe level operations, NAs in operations will make the result be NA.
        In row level operations, NAs will generally raise a TypeError.
        To avoid such behavior, fill the NAs before performing operations.
    """

    EXCLUDE_FROM_COMPARE = ('_column_expression', )

    def __init__(self, output_column: str, column_expression: str, column_type: Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.output_column = output_column
        self.column_expression = column_expression
        if column_type is not None and column_type not in SUPPORTED_TYPES:
            raise UnsupportedTypeError(f"Unsupported column type: '{column_type}'. It must be one of: {SUPPORTED_TYPES}")
        self.column_type = column_type
        self._column_expression = self.get_column_expression()

    def _validate_columns(self, df_columns):
        if self.strict and self.output_column in df_columns:
            raise ColumnAlreadyExistsError(f"Column {self.output_column} already exists in the input dataframe.")


class AddRowNumbersRule(UnaryOpBaseRule):
    """ Adds a new column with row numbers.

    Example::

        Given df:
        | A   | B  |
        | 1   | 2  |
        | 2   | 3  |
        | 3   | 4  |

    > AddRowNumbersRule("Row_Number").apply(df)

    Result::

        | A   | B  | Row_Number |
        | 1   | 2  | 0          |
        | 2   | 3  | 1          |
        | 3   | 4  | 2          |

    Args:
        output_column: The name of the new column to be added.
        start: The value to start the numbers from. Defaults to 0.
        step: The increment to be used between row numbers. Defaults to 1.

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
    """

    def __init__(self, output_column: str, start: int=0, step: int=1, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.output_column = output_column
        self.start = start
        self.step = step

    def _validate_columns(self, df_columns):
        if self.strict and self.output_column in df_columns:
            raise ColumnAlreadyExistsError(f"Column {self.output_column} already exists in the input dataframe.")
