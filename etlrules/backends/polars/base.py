from typing import Optional

from etlrules.data import RuleData
from etlrules.rule import ColumnsInOutMixin, UnaryOpBaseRule


class PolarsMixin:
    def assign_do_apply(self, df, input_column, output_column):
        return df.with_columns_seq(
            self.do_apply(df, df[input_column]).alias(output_column)
        )
