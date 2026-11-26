import logging

import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.strings import (
    StrLowerRule as StrLowerRuleBase,
    StrUpperRule as StrUpperRuleBase,
    StrCapitalizeRule as StrCapitalizeRuleBase,
    StrSplitRule as StrSplitRuleBase,
    StrSplitRejoinRule as StrSplitRejoinRuleBase,
    StrStripRule as StrStripRuleBase,
    StrPadRule as StrPadRuleBase,
    StrExtractRule as StrExtractRuleBase,
)
from etlrules.backends.pyarrow.base import PyArrowMixin, set_column

perf_logger = logging.getLogger("etlrules.perf")


class StrLowerRule(StrLowerRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return pc.utf8_lower(col)


class StrUpperRule(StrUpperRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return pc.utf8_upper(col)


class StrCapitalizeRule(StrCapitalizeRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return pc.utf8_capitalize(col)


def split(col, separator, limit):
    return pc.split_pattern(col, pattern=separator, max_splits=limit)


def sort_lists(lists, descending=False):
    """ Sorts the values inside each list (there's no list sort compute function). """
    if isinstance(lists, pa.ChunkedArray):
        lists = lists.combine_chunks()
    values = pc.list_flatten(lists)
    parents = pc.list_parent_indices(lists)
    order = pc.sort_indices(
        pa.table({"parent": parents, "value": values}),
        sort_keys=[("parent", "ascending"), ("value", "descending" if descending else "ascending")],
    )
    offsets = pc.subtract(lists.offsets, lists.offsets[0])
    return pa.ListArray.from_arrays(offsets, pc.take(values, order), mask=pc.is_null(lists))


class StrSplitRule(StrSplitRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return split(col, self.separator, self.limit)


class StrSplitRejoinRule(StrSplitRejoinRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        lists = split(col, self.separator, self.limit)
        if self.sort is not None:
            lists = sort_lists(lists, descending=self.sort == self.SORT_DESCENDING)
        return pc.binary_join(lists, self.new_separator)


class StrStripRule(StrStripRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        if self.characters is None:
            funcs = {self.STRIP_BOTH: pc.utf8_trim_whitespace, self.STRIP_RIGHT: pc.utf8_rtrim_whitespace,
                     self.STRIP_LEFT: pc.utf8_ltrim_whitespace}
            return funcs[self.how](col)
        funcs = {self.STRIP_BOTH: pc.utf8_trim, self.STRIP_RIGHT: pc.utf8_rtrim, self.STRIP_LEFT: pc.utf8_ltrim}
        return funcs[self.how](col, characters=self.characters)


class StrPadRule(StrPadRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        if self.how == self.PAD_LEFT:
            return pc.utf8_lpad(col, width=self.width, padding=self.fill_character)
        return pc.utf8_rpad(col, width=self.width, padding=self.fill_character)


def name_regex_groups(pattern: str) -> tuple:
    """ Names the unnamed groups in a regular expression (pyarrow's extract_regex only supports named groups).

    Returns:
        The new pattern and the names of all the groups, in order.
    """
    names = []
    out = []
    idx = 0
    in_class = False
    while idx < len(pattern):
        char = pattern[idx]
        if char == "\\":
            out.append(pattern[idx:idx + 2])
            idx += 2
            continue
        if in_class:
            if char == "]":
                in_class = False
        elif char == "[":
            in_class = True
            # a ] right after [ or [^ is a literal
            if pattern[idx + 1:idx + 2] == "]":
                out.append("[]")
                idx += 2
                continue
            if pattern[idx + 1:idx + 3] == "^]":
                out.append("[^]")
                idx += 3
                continue
        elif char == "(":
            if pattern.startswith("(?P<", idx):
                end = pattern.index(">", idx)
                names.append(pattern[idx + 4:end])
            elif pattern.startswith("(?<", idx) and not pattern.startswith(("(?<=", "(?<!"), idx):
                end = pattern.index(">", idx)
                names.append(pattern[idx + 3:end])
            elif not pattern.startswith("(?", idx):
                name = f"__etlrules_group_{len(names)}"
                names.append(name)
                out.append(f"(?P<{name}>")
                idx += 1
                continue
        out.append(char)
        idx += 1
    return "".join(out), names


class StrExtractRule(StrExtractRuleBase, PyArrowMixin):

    def apply(self, data):
        df = self._get_input_df(data)
        columns, output_columns = self.validate_columns_in_out(
            df.column_names, [self.input_column], self.output_columns, self.strict, validate_length=False)
        input_column = columns[0]
        pattern, names = name_regex_groups(self.regular_expression)
        extracted = pc.extract_regex(df[input_column], pattern=pattern)
        for idx, (name, output_column) in enumerate(zip(names, output_columns)):
            values = pc.struct_field(extracted, name)
            if idx == 0 and self.keep_original_value:
                values = pc.coalesce(values, df[input_column])
            df = set_column(df, output_column, values)
        self._set_output_df(data, df)
