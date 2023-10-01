from pandas import DataFrame
from typing import Optional, Sequence

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError


class PandasRuleValidationMixin:
    def assert_is_dataframe(self, df, context):
        assert isinstance(df, DataFrame), context


class ColumnsInOutMixin:
    def validate_columns_in(self, df: DataFrame, columns: Sequence[str], strict: bool) -> Sequence[str]:
        df_cols_set = set(df.columns)
        if strict:
            if not set(columns) <= df_cols_set:
                raise MissingColumnError(f"Column(s) {set(columns) - df_cols_set} are missing from the input dataframe.")
            return columns
        return [col for col in columns if col in df_cols_set]

    def validate_columns_out(self, df: DataFrame, columns: Sequence[str], output_columns: Optional[Sequence[str]], strict: bool) -> Sequence[str]:
        if output_columns:
            if strict:
                existing_columns = set(output_columns) <= set(df.columns)
                if existing_columns:
                    raise ColumnAlreadyExistsError(f"Column(s) already exist: {existing_columns}")
            if len(output_columns) != len(columns):
                raise ValueError(f"output_columns must be of the same length as the columns: {columns}")
        else:
            output_columns = columns
        return output_columns

    def validate_columns_in_out(self, df: DataFrame, columns: Sequence[str], output_columns: Optional[Sequence[str]], strict: bool) -> tuple[Sequence[str], Sequence[str]]:
        columns = self.validate_columns_in(df, columns, strict)
        output_columns = self.validate_columns_out(df, columns, output_columns, strict)
        return columns, output_columns
