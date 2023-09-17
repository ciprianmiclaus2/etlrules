from finrules.backends.common.basic import BaseProjectRule, BaseStartRule
from finrules.exceptions import MissingColumn
from finrules.rule import BaseRule

from .validation import PandasRuleValidationMixin


class StartRule(BaseStartRule, PandasRuleValidationMixin):
    ...


class ProjectRule(BaseProjectRule, PandasRuleValidationMixin):
    """ Reshapes the data frame to keep, eliminate or re-order the set of columns.

    Params:
        column_names: The list of columns to keep or eliminate from the data frame.
            The order of column names will be reflected in the result data frame, so this rule can be used to re-order columns.
        exclude: When set to True, the columns in the column_names will be excluded from the data frame. Boolean. Default: False
            In strict mode, if any column specified in the column_names doesn't exist in the input data frame, a MissingColumn exception is raised.
            In non strict mode, the missing columns are ignored.

    Common params:
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
        MissingColumn is raised in strict mode only, if any columns are missing from the input data frame.
    """

    def apply(self, data):
        """ Applies the rule to the input data.

        Params:
            data: An instance of RuleData which stores inputs and outputs, including the main outputs and any named inputs/outputs.

        Returns:
            None

        Note:
            apply doesn't return any data but it sets the results on the input data parameter (either main output or a named output depending on the rule set up).
        """
        df = self._get_input_df(data)
        remaining_columns = self._get_remaining_columns(df.columns)
        df = df[remaining_columns]
        self._set_output_df(data, df)


class RenameRule(BaseRule):
    """ Renames a set of columns in the data frame.

    Params:
        mapper: A dictionary of old names (keys) and new names (values) to be used for the rename operation
            The order of column names will be reflected in the result data frame, so this rule can be used to re-order columns.

    Common params:
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
        MissingColumn is raised in strict mode only, if any columns (keys) are missing from the input data frame.
    """

    def __init__(self, mapper, named_input=None, named_output=None, name=None, description=None, strict=True):
        assert isinstance(mapper, dict), "mapper needs to be a dict {old_name:new_name}"
        assert all(isinstance(key, str) and isinstance(val, str) for key, val in mapper.items()), "mapper needs to be a dict {old_name:new_name} where the names are str"
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.mapper = mapper

    def apply(self, data):
        """ Applies the rule to the input data.

        Params:
            data: An instance of RuleData which stores inputs and outputs, including the main outputs and any named inputs/outputs.

        Returns:
            None

        Note:
            apply doesn't return any data but it sets the results on the input data parameter (either main output or a named output depending on the rule set up).
        """
        df = self._get_input_df(data)
        if self.strict:
            if not set(self.mapper.keys()) <= set(df.columns):
                raise MissingColumn(f"Missing columns to rename: {set(self.mapper.keys()) - set(df.columns)}")
        df = df.rename(columns=self.mapper)
        self._set_output_df(data, df)


class SortRule(BaseRule):
    """ Sort the input dataframe by the given columns, either ascending or descending.

    Params:
        sort_by: Either a single column speified as a string or a list or tuple of columns to sort by
        ascending: Whether to sort ascending or descending. Boolean. Default: True

    Common params:
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

    Note:
        When multiple columns are specified, the first column decides the sort order.
        For any rows that have the same value in the first column, the second column is used to decide the sort order within that group and so on.
    """

    def __init__(self, sort_by, ascending=True, named_input=None, named_output=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        assert sort_by and (isinstance(sort_by, str) or (isinstance(sort_by, (list, tuple)) and all(isinstance(val, str) for val in sort_by))), "sort_by must be a str (single column) or a list of str (multiple columns)"
        self.sort_by = sort_by
        if isinstance(self.sort_by, str):
            self.sort_by = [self.sort_by]
        assert isinstance(ascending, bool) or (isinstance(ascending, (list, tuple)) and all(isinstance(val, bool) for val in ascending) and len(ascending) == len(self.sort_by)), "ascending must be a bool or a list of bool of the same len as sort_by"
        self.ascending = ascending

    def apply(self, data):
        """ Applies the rule to the input data.

        Params:
            data: An instance of RuleData which stores inputs and outputs, including the main outputs and any named inputs/outputs.

        Returns:
            None

        Note:
            apply doesn't return any data but it sets the results on the input data parameter (either main output or a named output depending on the rule set up).
        """
        df = self._get_input_df(data)
        df = df.sort_values(by=self.sort_by, ascending=self.ascending, ignore_index=True)
        self._set_output_df(data, df)


class DedupeRule(BaseRule):
    """ De-duplicates by dropping duplicates using a set of columns to determine the duplicates.
    It has logic to keep the first, last or none of the duplicate in a set of duplicates.

    Params:
        column_names: A subset of columns in the data frame which are used to determine the set of duplicates.
            Any rows that have the same values in these columns are considered to be duplicates.
        keep: What to keep in the de-duplication process. One of:
            first: keeps the first row in the duplicate set
            last: keeps the last row in the duplicate set
            none: drops all the duplicates

    Common params:
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
        MissingColumn is raised when a column specified to deduplicate on doesn't exist in the input data frame.

    Note:
        MissingColumn is raised in both strict and non-strict modes. This is because the rule cannot operate reliably without a correct set of columns.
    """

    KEEP_FIRST = 'first'
    KEEP_LAST = 'last'
    KEEP_NONE = 'none'

    ALL_KEEPS = (KEEP_FIRST, KEEP_LAST, KEEP_NONE)
 
    def __init__(self, column_names, keep=KEEP_FIRST, named_input=None, named_output=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=named_output, name=name, description=description, strict=strict)
        self.columns = [col for col in column_names]
        assert all(
            isinstance(col, str) for col in self.columns
        ), "DedupeRule: column_names must be strings"
        assert keep in self.ALL_KEEPS, f"DedupeRule: keep must be one of: {self.ALL_KEEPS}"
        self.keep = False if keep == DedupeRule.KEEP_NONE else keep

    def apply(self, data):
        """ Applies the rule to the input data.

        Params:
            data: An instance of RuleData which stores inputs and outputs, including the main outputs and any named inputs/outputs.

        Returns:
            None

        Note:
            apply doesn't return any data but it sets the results on the input data parameter (either main output or a named output depending on the rule set up).
        """
        df = self._get_input_df(data)
        if not set(self.columns) <= set(df.columns):
            raise MissingColumn(f"Missing column(s) to dedupe on: {set(self.columns) - set(df.columns)}")
        df = df.drop_duplicates(subset=self.columns, keep=self.keep, ignore_index=True)
        self._set_output_df(data, df)
