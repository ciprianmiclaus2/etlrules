from pandas import DataFrame
from typing import Optional, Sequence

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError


class PandasRuleValidationMixin:
    def assert_is_dataframe(self, df, context):
        assert isinstance(df, DataFrame), context


class ColumnsInOutMixin:
    def validate_input_column(self, df: DataFrame, input_column: str, strict: bool):
        if input_column not in set(df.columns):
            raise MissingColumnError(f"Column '{input_column}' is missing from the input dataframe.")
        return input_column

    def validate_output_column(self, df: DataFrame, input_column: str, output_column: Optional[str], strict: bool):
        if output_column is not None:
            if strict and output_column in set(df.columns):
                raise ColumnAlreadyExistsError(f"Column '{output_column}' already exists in the input dataframe.")
            return output_column
        return input_column

    def validate_in_out_columns(self, df: DataFrame, input_column: str, output_column: Optional[str], strict: bool):
        input_column = self.validate_input_column(df, input_column, strict)
        output_column = self.validate_output_column(df, input_column, output_column, strict)
        return input_column, output_column

    def validate_columns_in(self, df: DataFrame, columns: Sequence[str], strict: bool) -> Sequence[str]:
        df_cols_set = set(df.columns)
        if not set(columns) <= df_cols_set:
            raise MissingColumnError(f"Column(s) {set(columns) - df_cols_set} are missing from the input dataframe.")
        return columns

    def validate_columns_out(self, df: DataFrame, columns: Sequence[str], output_columns: Optional[Sequence[str]], strict: bool, validate_length: bool=True) -> Sequence[str]:
        if output_columns:
            if strict:
                existing_columns = set(output_columns) & set(df.columns)
                if existing_columns:
                    raise ColumnAlreadyExistsError(f"Column(s) already exist: {existing_columns}")
            if validate_length and len(output_columns) != len(columns):
                raise ValueError(f"output_columns must be of the same length as the columns: {columns}")
        else:
            output_columns = columns
        return output_columns

    def validate_columns_in_out(self, df: DataFrame, columns: Sequence[str], output_columns: Optional[Sequence[str]], strict: bool, validate_length: bool=True) -> tuple[Sequence[str], Sequence[str]]:
        columns = self.validate_columns_in(df, columns, strict)
        output_columns = self.validate_columns_out(df, columns, output_columns, strict, validate_length=validate_length)
        return columns, output_columns
