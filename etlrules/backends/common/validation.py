from typing import Iterable, Optional

from etlrules.backends.common.dataframe import get_column_names
from etlrules.exceptions import MissingColumnError, ValidationError
from etlrules.rule import UnaryOpBaseRule


class BaseValidationRule(UnaryOpBaseRule):
    """ Base class for the rules which check the data and raise ValidationError if the check fails.

    The data is not changed: the output of the rule is the input dataframe.
    """

    def __init__(self, columns: Optional[Iterable[str]]=None, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in columns] if columns is not None else None
        assert self.columns is None or (self.columns and all(isinstance(col, str) for col in self.columns)), "columns must be a non-empty list of column names or None."

    def validate(self, df, columns) -> None:
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        df = self._get_input_df(data)
        df_columns = get_column_names(df)
        columns = self.columns or df_columns
        if not set(columns) <= set(df_columns):
            raise MissingColumnError(f"Column(s) {set(columns) - set(df_columns)} are missing from the input dataframe.")
        self.validate(df, columns)
        self._set_output_df(data, df)


class AssertNotNullRule(BaseValidationRule):
    """ Checks that a set of columns don't have missing values (NA/nulls) and raises ValidationError if they do.

    The data is not changed: the output of the rule is the input dataframe. Use it to stop a plan early when the
    data doesn't match the expectations (e.g. before loading it into a database).

    Basic usage:

        # raises ValidationError if any of the columns Id or Name have missing values
        rule = AssertNotNullRule(["Id", "Name"])
        rule.apply(data)

    Args:
        columns (Optional[Iterable[str]]): The columns to check. Optional.
            When not specified, all the columns are checked.

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
        strict (bool): When set to True, the rule does a stricter validation. Default: True

    Raises:
        ValidationError: raised if any of the columns have missing values. The message has the number of
            missing values in each column.
        MissingColumnError: raised if any of the columns are missing from the input dataframe.

    Note:
        With the dask backend, the check computes the input dataframe.
    """

    def count_nulls(self, df, columns) -> dict:
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def validate(self, df, columns):
        counts = {col: int(count) for col, count in self.count_nulls(df, columns).items() if count}
        if counts:
            details = ", ".join(f"{col}: {count}" for col, count in counts.items())
            raise ValidationError(f"Missing values found in column(s) ({details}).")


class AssertUniqueRule(BaseValidationRule):
    """ Checks that the values of a column (or the combinations of values of a set of columns) are unique and
    raises ValidationError if they're not.

    The data is not changed: the output of the rule is the input dataframe. Use it, for example, to check that a
    key is unique before loading the data into a database.

    Basic usage:

        # raises ValidationError if the values in the Id column are not unique
        rule = AssertUniqueRule(["Id"])
        rule.apply(data)

        # raises ValidationError if there are rows with the same Date and Country
        rule = AssertUniqueRule(["Date", "Country"])
        rule.apply(data)

    Args:
        columns (Optional[Iterable[str]]): The columns which together must be unique. Optional.
            When not specified, all the columns are used (ie the rows must be unique).

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
        strict (bool): When set to True, the rule does a stricter validation. Default: True

    Raises:
        ValidationError: raised if there are duplicates. The message has the number of duplicate rows.
        MissingColumnError: raised if any of the columns are missing from the input dataframe.

    Note:
        Missing values (NA) are compared like any other value: two rows with missing values in the same columns
        (and the same values in the other columns) are duplicates.

    Note:
        With the dask backend, the check computes the input dataframe.
    """

    def count_duplicates(self, df, columns) -> int:
        """ Returns the number of rows which are duplicates of a previous row. """
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def validate(self, df, columns):
        duplicates = int(self.count_duplicates(df, columns))
        if duplicates:
            raise ValidationError(f"Found {duplicates} duplicate row(s) on column(s) {columns}.")
