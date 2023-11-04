import polars as pl
import re

from etlrules.backends.common.basic import (
    DedupeRule as DedupeRuleBase,
    RenameRule as RenameRuleBase,
    ReplaceRule as ReplaceRuleBase,
    SortRule as SortRuleBase,
)

from .base import PolarsMixin


class DedupeRule(DedupeRuleBase):
    def do_dedupe(self, df):
        return df.unique(subset=self.columns, keep=self.keep, maintain_order=True)


class RenameRule(RenameRuleBase):
    def do_rename(self, df, mapper):
        return df.rename(mapper)


class SortRule(SortRuleBase):
    def do_sort(self, df):
        if isinstance(self.ascending, bool):
            descending = not self.ascending
        else:
            descending = [not asc for asc in self.ascending]
        return df.sort(by=self.sort_by, descending=descending)


class ReplaceRule(ReplaceRuleBase, PolarsMixin):

    def _get_old_new_regex(self, old_val, new_val):
        compiled = re.compile(old_val)
        groupindex = compiled.groupindex
        if compiled.groups > 0 and not groupindex:
            groupindex = {v: v for v in range(1, compiled.groups + 1)}
        for group_name, group_idx in groupindex.items():
            new_val = new_val.replace(f"${group_name}", f"${{{group_name}}}")
            new_val = new_val.replace(f"\\g<{group_name}>", f"${{{group_name}}}")
            new_val = new_val.replace(f"\\{group_idx}", f"${{{group_idx}}}")
        return old_val, new_val

    def do_apply(self, df, col):
        if self.regex:
            for old_val, new_val in zip(self.values, self.new_values):
                old_val, new_val = self._get_old_new_regex(old_val, new_val)
                col = col.str.replace(old_val, new_val)
        else:
            col = col.map_dict(dict(zip(self.values, self.new_values)), default=pl.first())
        return col
