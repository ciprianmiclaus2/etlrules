from finrules.exceptions import MissingColumn
from finrules.rule import BinaryOpBaseRule


class BaseJoinRule(BinaryOpBaseRule):

    JOIN_TYPE = None

    def __init__(self, named_input_left, named_input_right, key_columns_left, key_columns_right=None, suffixes=(None, "_r"), named_output=None, name=None, description=None, strict=True):
        super().__init__(named_input_left=named_input_left, named_input_right=named_input_right, named_output=named_output, name=name, description=description, strict=strict)
        assert isinstance(key_columns_left, (list, tuple)) and key_columns_left and all(isinstance(col, str) for col in key_columns_left), "JoinRule: key_columns_left must a non-empty list of tuple with str column names"
        self.key_columns_left = key_columns_left
        self.key_columns_right = key_columns_right or key_columns_left
        assert isinstance(suffixes, (list, tuple)) and len(suffixes) == 2 and all(s is None or isinstance(s, str) for s in suffixes), "The suffixes must be a list or tuple of 2 elements"
        self.suffixes = suffixes

    def apply(self, data):
        assert self.JOIN_TYPE in {"left", "right", "outer", "inner"}
        super().apply(data)
        left_df = self._get_input_df_left(data)
        right_df = self._get_input_df_right(data)
        if not set(self.key_columns_left) <= set(left_df.columns):
            raise MissingColumn(f"Missing columns in join in the left dataframe: {set(self.key_columns_left) - set(left_df.columns)}")
        if not set(self.key_columns_right) <= set(right_df.columns):
            raise MissingColumn(f"Missing columns in join in the right dataframe: {set(self.key_columns_right) - set(right_df.columns)}")
        df = left_df.merge(
            right_df,
            how=self.JOIN_TYPE,
            left_on=self.key_columns_left,
            right_on=self.key_columns_right,
            suffixes=self.suffixes
        )
        self._set_output_df(data, df)


class LeftJoinRule(BaseJoinRule):
    """ Performs a database-style left join operation on two data frames.

    A join involves two data frames left_df <join> right_df with the result performing a
    database style join or a merge of the two, with the resulting columns coming from both
    dataframes.
    For example, if the left dataframe has two columns A, B and the right dataframe has two
    column A, C, and assuming A is the key column the result will have three columns A, B, C.
    The rows that have the same value in the key column A will be merged on the same row in the
    result dataframe.

    A left join specifies that all the rows in the left dataframe will be present in the result,
    irrespective of whether there's a corresponding row with the same values in the key columns in
    the right dataframe. The right columns will be populated with NaNs/None when there is no
    corresponding row on the right.

    E.g.

    left:
        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right:
        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"])
        | A  | B  | C  |
        | 1  | a  | c  |
        | 2  | b  | NA |

    Args:
        named_input_left: Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right: Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left: A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right: A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes: A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output: Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name: Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict: When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumn is raised if any columns (keys) are missing from any of the two input data frames.
    """

    JOIN_TYPE = "left"


class InnerJoinRule(BaseJoinRule):
    """ Performs a database-style inner join operation on two data frames.

    A join involves two data frames left_df <join> right_df with the result performing a
    database style join or a merge of the two, with the resulting columns coming from both
    dataframes.
    For example, if the left dataframe has two columns A, B and the right dataframe has two
    column A, C, and assuming A is the key column the result will have three columns A, B, C.
    The rows that have the same value in the key column A will be merged on the same row in the
    result dataframe.

    An inner join specifies that only those rows that have key values in both left and right
    will be copied over and merged into the result data frame. Any rows without corresponding
    values on the other side (be it left or right) will be dropped from the result.

    E.g.

    left:
        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right:
        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"])
        | A  | B  | C  |
        | 1  | a  | c  |

    Args:
        named_input_left: Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right: Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left: A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right: A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes: A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output: Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name: Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict: When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumn is raised if any columns (keys) are missing from any of the two input data frames.
    """

    JOIN_TYPE = "inner"


class OuterJoinRule(BaseJoinRule):
    """ Performs a database-style left join operation on two data frames.

    A join involves two data frames left_df <join> right_df with the result performing a
    database style join or a merge of the two, with the resulting columns coming from both
    dataframes.
    For example, if the left dataframe has two columns A, B and the right dataframe has two
    column A, C, and assuming A is the key column the result will have three columns A, B, C.
    The rows that have the same value in the key column A will be merged on the same row in the
    result dataframe.

    An outer join specifies that all the rows in the both left and right dataframes will be present
    in the result, irrespective of whether there's a corresponding row with the same values in the
    key columns in the other dataframe. The missing side will have its columns populated with NA
    when the rows are missing.

    E.g.

    left:
        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right:
        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"])
        | A  | B  | C  |
        | 1  | a  | c  |
        | 2  | b  | NA |
        | 3  | NA | d  |

    Args:
        named_input_left: Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right: Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left: A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right: A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes: A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output: Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name: Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict: When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumn is raised if any columns (keys) are missing from any of the two input data frames.
    """

    JOIN_TYPE = "outer"


class RightJoinRule(BaseJoinRule):
    """ Performs a database-style left join operation on two data frames.

    A join involves two data frames left_df <join> right_df with the result performing a
    database style join or a merge of the two, with the resulting columns coming from both
    dataframes.
    For example, if the left dataframe has two columns A, B and the right dataframe has two
    column A, C, and assuming A is the key column the result will have three columns A, B, C.
    The rows that have the same value in the key column A will be merged on the same row in the
    result dataframe.

    A right join specifies that all the rows in the right dataframe will be present in the result,
    irrespective of whether there's a corresponding row with the same values in the key columns in
    the left dataframe. The left columns will be populated with NA when there is no
    corresponding row on the left.

    E.g.

    left:
        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right:
        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"])
        | A  | B  | C  |
        | 1  | a  | c  |
        | 3  | NA | d  |

    Note:
        A right join is equivalent to a left join with the dataframes inverted, ie:
        left_df <left_join> right_df
        is equivalent to
        right_df <right_join> left_df
        although the order of the rows will be different.

    Args:
        named_input_left: Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right: Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left: A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right: A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes: A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output: Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name: Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description: Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict: When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumn is raised if any columns (keys) are missing from any of the two input data frames.
    """

    JOIN_TYPE = "right"
