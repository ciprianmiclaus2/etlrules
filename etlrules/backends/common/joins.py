from typing import Iterable, Optional

from etlrules.exceptions import MissingColumnError
from etlrules.rule import BinaryOpBaseRule
from etlrules.backends.common.dataframe import get_column_names


class BaseJoinRule(BinaryOpBaseRule):

    JOIN_TYPE = None

    def __init__(self, named_input_left: Optional[str], named_input_right: Optional[str], key_columns_left: Iterable[str], key_columns_right: Optional[Iterable[str]]=None, suffixes: Iterable[Optional[str]]=(None, "_r"), named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input_left=named_input_left, named_input_right=named_input_right, named_output=named_output, name=name, description=description, strict=strict)
        assert isinstance(key_columns_left, (list, tuple)) and key_columns_left and all(isinstance(col, str) for col in key_columns_left), "JoinRule: key_columns_left must a non-empty list of tuple with str column names"
        self.key_columns_left = [col for col in key_columns_left]
        self.key_columns_right = [col for col in key_columns_right] if key_columns_right is not None else None
        assert isinstance(suffixes, (list, tuple)) and len(suffixes) == 2 and all(s is None or isinstance(s, str) for s in suffixes), "The suffixes must be a list or tuple of 2 elements"
        self.suffixes = suffixes

    def _get_key_columns(self):
        return self.key_columns_left, self.key_columns_right or self.key_columns_left

    def do_apply(self, left_df, right_df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        assert self.JOIN_TYPE in {"left", "right", "outer", "inner"}
        super().apply(data)
        left_df = self._get_input_df_left(data)
        right_df = self._get_input_df_right(data)
        left_on, right_on = self._get_key_columns()
        if not set(left_on) <= set(get_column_names(left_df)):
            raise MissingColumnError(f"Missing columns in join in the left dataframe: {set(left_on) - set(get_column_names(left_df))}")
        if not set(right_on) <= set(get_column_names(right_df)):
            raise MissingColumnError(f"Missing columns in join in the right dataframe: {set(right_on) - set(get_column_names(right_df))}")
        df = self.do_apply(left_df, right_df)
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

    Example:

    left dataframe:

        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right dataframe:

        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"]):

        | A  | B  | C  |
        | 1  | a  | c  |
        | 2  | b  | NA |

    Args:
        named_input_left (Optional[str]): Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right (Optional[str]): Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left (Iterable[str]): A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right (Optional[Iterable[str]]): A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes (Iterable[Optional[str]]): A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised if any columns (keys) are missing from any of the two input data frames.
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

    Example:

    left dataframe:

        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right dataframe:

        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"]):

        | A  | B  | C  |
        | 1  | a  | c  |

    Args:
        named_input_left (Optional[str]): Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right (Optional[str]): Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left (Iterable[str]): A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right (Optional[Iterable[str]]): A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes (Iterable[Optional[str]]): A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised if any columns (keys) are missing from any of the two input data frames.
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

    Example:

    left dataframe:

        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right dataframe:

        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"]):

        | A  | B  | C  |
        | 1  | a  | c  |
        | 2  | b  | NA |
        | 3  | NA | d  |

    Args:
        named_input_left (Optional[str]): Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right (Optional[str]): Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left (Iterable[str]): A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right (Optional[Iterable[str]]): A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes (Iterable[Optional[str]]): A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised if any columns (keys) are missing from any of the two input data frames.
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

    Example:

    left dataframe:

        | A  | B  |
        | 1  | a  |
        | 2  | b  |

    right dataframe:

        | A  | C  |
        | 1  | c  |
        | 3  | d  |

    result (key columns=["A"]):

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
        named_input_left (Optional[str]): Which dataframe to use as the input on the left side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        named_input_right (Optional[str]): Which dataframe to use as the input on the right side of the join.
            When set to None, the input is taken from the main output of the previous rule.
            Set it to a string value, the name of an output dataframe of a previous rule.
        key_columns_left (Iterable[str]): A list or tuple of column names to join on (columns in the left data frame)
        key_columns_right (Optional[Iterable[str]]): A list or tuple of column names to join on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.
        suffixes (Iterable[Optional[str]]): A list or tuple of two values which will be set as suffixes for the columns in the
            result data frame for those columns that have the same name (and are not key columns).

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        MissingColumnError: raised if any columns (keys) are missing from any of the two input data frames.
    """

    JOIN_TYPE = "right"


class BaseFilterJoinRule(BinaryOpBaseRule):
    """ Base class for the joins which filter the rows of the left dataframe (semi and anti joins). """

    def __init__(self, named_input_left: Optional[str], named_input_right: Optional[str], key_columns_left: Iterable[str], key_columns_right: Optional[Iterable[str]]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_input_left=named_input_left, named_input_right=named_input_right, named_output=named_output, name=name, description=description, strict=strict)
        self.key_columns_left = [col for col in key_columns_left]
        assert self.key_columns_left and all(isinstance(col, str) for col in self.key_columns_left), "key_columns_left must be a non-empty list of column names."
        self.key_columns_right = [col for col in key_columns_right] if key_columns_right is not None else None
        assert self.key_columns_right is None or (
            len(self.key_columns_right) == len(self.key_columns_left) and all(isinstance(col, str) for col in self.key_columns_right)
        ), "key_columns_right must have the same number of column names as key_columns_left."

    def _get_key_columns(self):
        return self.key_columns_left, self.key_columns_right or self.key_columns_left

    def do_apply(self, left_df, right_df):
        raise NotImplementedError("Have you imported the rules from etlrules.backends.<your_backend> and not common?")

    def apply(self, data):
        super().apply(data)
        left_df = self._get_input_df_left(data)
        right_df = self._get_input_df_right(data)
        left_on, right_on = self._get_key_columns()
        if not set(left_on) <= set(get_column_names(left_df)):
            raise MissingColumnError(f"Missing columns in join in the left dataframe: {set(left_on) - set(get_column_names(left_df))}")
        if not set(right_on) <= set(get_column_names(right_df)):
            raise MissingColumnError(f"Missing columns in join in the right dataframe: {set(right_on) - set(get_column_names(right_df))}")
        df = self.do_apply(left_df, right_df)
        self._set_output_df(data, df)


class SemiJoinRule(BaseFilterJoinRule):
    """ Keeps the rows of the left dataframe which have a match in the right dataframe.

    Unlike an inner join, the result has only the columns of the left dataframe and each left row appears at most
    once, even when it matches multiple rows in the right dataframe. The order of the left rows is kept.

    Example:

    left dataframe:

        | A  | B  |
        | 1  | a  |
        | 2  | b  |
        | 3  | c  |

    right dataframe:

        | A  | C  |
        | 1  | x  |
        | 1  | y  |
        | 3  | z  |

    result (key columns=["A"]):

        | A  | B  |
        | 1  | a  |
        | 3  | c  |

    Args:
        named_input_left (Optional[str]): Which dataframe to use as the left input (the rows to keep or remove).
            When set to None, the input is taken from the main output of the previous rule.
        named_input_right (Optional[str]): Which dataframe to use as the right input (the rows to match against).
            When set to None, the input is taken from the main output of the previous rule.
        key_columns_left (Iterable[str]): A list or tuple of column names to match on (columns in the left data frame)
        key_columns_right (Optional[Iterable[str]]): A list or tuple of column names to match on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter validation. Default: True

    Raises:
        MissingColumnError: raised if any columns (keys) are missing from any of the two input data frames.

    Note:
        Like in SQL, missing values (NA) in the key columns don't match anything.
    """


class AntiJoinRule(BaseFilterJoinRule):
    """ Keeps the rows of the left dataframe which don't have a match in the right dataframe.

    This is useful, for example, to find the new records which have not been loaded yet. The result has only the
    columns of the left dataframe and the order of the left rows is kept.

    Example:

    left dataframe:

        | A  | B  |
        | 1  | a  |
        | 2  | b  |
        | 3  | c  |

    right dataframe:

        | A  | C  |
        | 1  | x  |
        | 3  | z  |

    result (key columns=["A"]):

        | A  | B  |
        | 2  | b  |

    Args:
        named_input_left (Optional[str]): Which dataframe to use as the left input (the rows to keep or remove).
            When set to None, the input is taken from the main output of the previous rule.
        named_input_right (Optional[str]): Which dataframe to use as the right input (the rows to match against).
            When set to None, the input is taken from the main output of the previous rule.
        key_columns_left (Iterable[str]): A list or tuple of column names to match on (columns in the left data frame)
        key_columns_right (Optional[Iterable[str]]): A list or tuple of column names to match on (columns in the right data frame).
            If not set or set to None, the key_columns_left is used on the right dataframe too.

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter validation. Default: True

    Raises:
        MissingColumnError: raised if any columns (keys) are missing from any of the two input data frames.

    Note:
        Like in SQL, missing values (NA) in the key columns don't match anything, so the left rows with missing
        values in the key columns are kept.
    """
