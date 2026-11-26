from typing import Iterable, Literal, Optional, Union

from etlrules.backends.common.dataframe import get_column_names
from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError
from etlrules.rule import UnaryOpBaseRule


class FillNullRule(UnaryOpBaseRule):
    """ Replaces the missing values (NA/nulls) in a set of columns with a given value.

    Basic usage:

        # replaces the missing values in columns A and B with 0
        rule = FillNullRule(["A", "B"], 0)
        rule.apply(data)

        # replaces the missing values in column C with "unknown"
        rule = FillNullRule(["C"], "unknown")
        rule.apply(data)

    Example:

        Given df:
        | A    | B    |
        | 1    | NA   |
        | NA   | 2    |

    > FillNullRule(["A", "B"], 0).apply(df)

    Result:

        | A   | B  |
        | 1   | 0  |
        | 0   | 2  |

    Args:
        columns: The columns in which to replace the missing values.
            The rest of the columns in the dataframe are not affected.
        value: The value to replace the missing values with. It should be of a type compatible with the columns
            (e.g. a number for numeric columns, a string for string columns).

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
        strict: When set to True, the rule does a stricter validation. Default: True

    Raises:
        MissingColumnError: raised if any of the columns are missing from the input dataframe.

    See also:
        ForwardFillRule and BackFillRule fill the missing values with values from other rows.
    """

    def __init__(self, columns: Iterable[str], value: Union[int, float, str, bool], named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        assert self.columns, "FillNullRule: at least one column must be specified."
        assert all(isinstance(col, str) for col in self.columns), "FillNullRule: columns must be strings."
        assert value is not None, "FillNullRule: value cannot be None."
        self.value = value

    def do_fill(self, df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df_columns = set(get_column_names(df))
        if not set(self.columns) <= df_columns:
            raise MissingColumnError(f"Column(s) {set(self.columns) - df_columns} are missing from the input dataframe.")
        df = self.do_fill(df)
        self._set_output_df(data, df)


class DropNullRowsRule(UnaryOpBaseRule):
    """ Removes the rows with missing values (NA/nulls).

    Basic usage:

        # removes the rows with a missing value in any of the columns
        rule = DropNullRowsRule()
        rule.apply(data)

        # removes the rows with a missing value in column A or in column B
        rule = DropNullRowsRule(["A", "B"])
        rule.apply(data)

        # removes the rows where both columns A and B have missing values
        rule = DropNullRowsRule(["A", "B"], how="all")
        rule.apply(data)

    Args:
        columns: The columns to check for missing values. Optional.
            When not specified, all the columns are checked.
        how: One of:
            any: a row is removed if any of the columns has a missing value (default)
            all: a row is removed only if all the columns have missing values

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
        strict: When set to True, the rule does a stricter validation. Default: True

    Raises:
        MissingColumnError: raised if any of the columns are missing from the input dataframe.
    """

    HOW_ANY = "any"
    HOW_ALL = "all"

    def __init__(self, columns: Optional[Iterable[str]]=None, how: Literal["any", "all"]="any", named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns] if columns is not None else None
        assert self.columns is None or all(isinstance(col, str) for col in self.columns), "DropNullRowsRule: columns must be strings."
        assert how in (self.HOW_ANY, self.HOW_ALL), f"DropNullRowsRule: how must be one of: {self.HOW_ANY}, {self.HOW_ALL}."
        self.how = how

    def do_drop(self, df, columns):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df_columns = get_column_names(df)
        columns = self.columns if self.columns else df_columns
        if not set(columns) <= set(df_columns):
            raise MissingColumnError(f"Column(s) {set(columns) - set(df_columns)} are missing from the input dataframe.")
        df = self.do_drop(df, columns)
        self._set_output_df(data, df)


class CoalesceRule(UnaryOpBaseRule):
    """ Creates a column with the first non-missing value from a list of columns.

    Basic usage:

        # Phone is the mobile number, or the home number if the mobile is missing, or the work number
        rule = CoalesceRule(["Mobile", "Home", "Work"], "Phone")
        rule.apply(data)

    Example:

        Given df:
        | Mobile | Home | Work |
        | 111    | 222  | NA   |
        | NA     | 444  | 555  |
        | NA     | NA   | 666  |
        | NA     | NA   | NA   |

    > CoalesceRule(["Mobile", "Home", "Work"], "Phone").apply(df)

    Result:

        | Mobile | Home | Work | Phone |
        | 111    | 222  | NA   | 111   |
        | NA     | 444  | 555  | 444   |
        | NA     | NA   | 666  | 666   |
        | NA     | NA   | NA   | NA    |

    Args:
        columns: The columns to take the values from, in order of preference.
            The columns should have the same (or compatible) types.
        output_column: The name of the new column with the result.

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
        strict: When set to True, the rule does a stricter validation. Default: True

    Raises:
        MissingColumnError: raised if any of the columns are missing from the input dataframe.
        ColumnAlreadyExistsError: raised in strict mode only if the output_column already exists in the dataframe.

    Note:
        In non-strict mode, an existing output_column is overwritten.
    """

    def __init__(self, columns: Iterable[str], output_column: str, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns]
        assert self.columns, "CoalesceRule: at least one column must be specified."
        assert all(isinstance(col, str) for col in self.columns), "CoalesceRule: columns must be strings."
        assert output_column and isinstance(output_column, str), "CoalesceRule: output_column must be a non-empty string."
        self.output_column = output_column

    def do_coalesce(self, df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df_columns = set(get_column_names(df))
        if not set(self.columns) <= df_columns:
            raise MissingColumnError(f"Column(s) {set(self.columns) - df_columns} are missing from the input dataframe.")
        if self.strict and self.output_column in df_columns:
            raise ColumnAlreadyExistsError(f"Column '{self.output_column}' already exists in the input dataframe.")
        df = self.do_coalesce(df)
        self._set_output_df(data, df)
