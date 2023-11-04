from typing import Optional, Sequence, Union

from etlrules.backends.common.base import BaseAssignColumnRule


class RoundRule(BaseAssignColumnRule):
    """ Rounds a set of columns to specified decimal places.

    Basic usage::

        # rounds Col_A to 2dps
        rule = RoundRule("Col_A", 2)
        rule.apply(data)

        # rounds Col_B to 0dps and output the results into Col_C, Col_B remains unchanged
        rule = RoundRule("Col_B", 0, output_column="Col_C")
        rule.apply(data)

    Args:
        input_column: A column with values to round as per the specified scale.
        scale: An integer specifying the number of decimal places to round to.
        output_column (Optional[str]): An optional name for a new column with the rounded values.
            If provided, the existing column is unchanged and the new column is created with the results.
            If not provided, the result is updated in place.

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
        MissingColumnError: raised if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """

    def __init__(self, input_column: str, scale: Union[int, Sequence[int]], output_column: Optional[str]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(input_column=input_column, output_column=output_column, named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert isinstance(scale, int), "scale must be an integer value"
        self.scale = scale


class AbsRule(BaseAssignColumnRule):
    """ Converts numbers to absolute values.

    Basic usage::

        rule = AbsRule("col_A")
        rule.apply(data)

    Args:
        input_column (str): The name of the column to convert to absolute values.
        output_column (Optional[str]): An optional new column with the absolute values.
            If provided the existing column is unchanged and a new column is created with the absolute values.
            If not provided, the result is updated in place.

        named_input (Optional[str]): Which dataframe to use as the input. Optional.
            When not set, the input is taken from the main output.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised if a column doesn't exist in the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, the overwriting of existing columns is ignored.
    """
