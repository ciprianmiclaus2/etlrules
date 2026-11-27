from etlrules.backends.common.numeric import RoundRule as RoundRuleBase, AbsRule as AbsRuleBase, ClipRule as ClipRuleBase

from .base import PolarsMixin


class RoundRule(RoundRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        return col.round(self.scale)


class AbsRule(AbsRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        return col.abs()


class ClipRule(ClipRuleBase, PolarsMixin):

    def do_apply(self, df, col):
        return col.clip(lower_bound=self.lower, upper_bound=self.upper)
