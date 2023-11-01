from typing import Iterable, Optional

from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError, SchemaError
from etlrules.rule import BinaryOpBaseRule


class VConcatRule(BinaryOpBaseRule):
    """ Vertically concatenates two dataframe with the result having the rows from the left dataframe followed by the rows from the right dataframe.

    The rows of the right dataframe are added at the bottom of the rows from the left dataframe in the result dataframe.

    Example::

        Left dataframe:
        | A   | B  |
        | a   | 1  |
        | b   | 2  |
        | c   | 3  |

        Right dataframe:
        | A   | B  |
        | d   | 4  |
        | e   | 5  |
        | f   | 6  |  

    After a concat(left, right), the result will look like::

        | A   | B  |
        | a   | 1  |
        | b   | 2  |
        | c   | 3  |
        | d   | 4  |
        | e   | 5  |
        | f   | 6  |

    Args:
        named_input_left: Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right: Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        subset_columns: A subset list of columns available in both dataframes.
            Only these columns will be concated.
            The effect is similar to doing a ProjectRule(subset_columns) on both dataframes before the concat.

        named_output: Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name: Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict: When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised if any subset columns specified are missing from any of the dataframe.
        SchemaError: raised in strict mode only if the columns differ between the two dataframes and subset_columns is not specified.
    
    Note:
        In strict mode, as described above, SchemaError is raised if the columns are not the same (names, types can be inferred).
        In non-strict mode, columns are not checked and values are filled with NA when missing.
    """

    def __init__(self, named_input_left: Optional[str], named_input_right: Optional[str], subset_columns: Optional[Iterable[str]]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input_left=named_input_left, named_input_right=named_input_right, named_output=named_output, name=name, description=description, strict=strict)
        self.subset_columns = [col for col in subset_columns] if subset_columns is not None else None

    def do_concat(self, left_df, right_df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        left_df = self._get_input_df_left(data)
        right_df = self._get_input_df_right(data)
        if self.subset_columns:
            if not set(self.subset_columns) <= set(left_df.columns):
                raise MissingColumnError(f"Missing columns in the left dataframe of the concat operation: {set(self.subset_columns) - set(left_df.columns)}")
            if not set(self.subset_columns) <= set(right_df.columns):
                raise MissingColumnError(f"Missing columns in the right dataframe of the concat operation: {set(self.subset_columns) - set(right_df.columns)}")
            left_df = left_df[self.subset_columns]
            right_df = right_df[self.subset_columns]
        if self.strict:
            if set(left_df.columns) != set(right_df.columns):
                raise SchemaError(f"VConcat needs both dataframe have the same schema. Missing columns in the right df: {set(right_df.columns) - set(left_df.columns)}. Missing columns in the left df: {set(left_df.columns) - set(right_df.columns)}")
        df = self.do_concat(left_df, right_df)
        self._set_output_df(data, df)


class HConcatRule(BinaryOpBaseRule):
    """ Horizontally concatenates two dataframe with the result having the columns from the left dataframe followed by the columns from the right dataframe.

    The columns from the left dataframe will be followed by the columns from the right dataframe in the result dataframe.
    The two dataframes must not have columns with the same name.

    Example::

        Left dataframe:
        | A   | B  |
        | a   | 1  |
        | b   | 2  |
        | c   | 3  |

        Right dataframe:
        | C   | D  |
        | d   | 4  |
        | e   | 5  |
        | f   | 6  |  

    After a concat(left, right), the result will look like::

        | A   | B  | C   | D  |
        | a   | 1  | d   | 4  |
        | b   | 2  | e   | 5  |
        | c   | 3  | f   | 6  |

    Args:
        named_input_left: Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right: Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
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
        ColumnAlreadyExistsError: raised if the two dataframes have columns with the same name.
        SchemaError: raised in strict mode only if the two dataframes have different number of rows.
    """

    def __init__(self, named_input_left: Optional[str], named_input_right: Optional[str], named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        # This __init__ not really needed but the type annotations are extracted from it
        super().__init__(named_input_left=named_input_left, named_input_right=named_input_right, named_output=named_output, name=name, description=description, strict=strict)

    def do_concat(self, left_df, right_df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        left_df = self._get_input_df_left(data)
        right_df = self._get_input_df_right(data)
        overlapping_names = set(left_df.columns) & set(right_df.columns)
        if overlapping_names:
            raise ColumnAlreadyExistsError(f"Column(s) {overlapping_names} exist in both dataframes.")
        if self.strict:
            if len(left_df) != len(right_df):
                raise SchemaError(f"HConcat needs the two dataframe to have the same number of rows. left df={len(left_df)} rows, right df={len(right_df)} rows.")
        df = self.do_concat(left_df, right_df)
        self._set_output_df(data, df)
