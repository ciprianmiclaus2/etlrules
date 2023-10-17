from typing import Optional

from etlrules.backends.pandas.expressions import Expression
from etlrules.exceptions import ColumnAlreadyExistsError
from etlrules.rule import UnaryOpBaseRule


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
        TypeError: raised if an operation is not supported between the types involved
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

    def __init__(self, output_column: str, column_expression: str, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.output_column = output_column
        self.column_expression = column_expression
        self._column_expression = Expression(self.column_expression, filename=f'{self.output_column}_expression.py')

    def apply(self, data):
        df = self._get_input_df(data)
        if self.strict and self.output_column in df.columns:
            raise ColumnAlreadyExistsError(f"Column {self.output_column} already exists in the input dataframe.")
        result = self._column_expression.eval(df)
        df = df.assign(**{self.output_column: result})
        self._set_output_df(data, df)
