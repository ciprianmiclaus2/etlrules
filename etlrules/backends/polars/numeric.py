from etlrules.backends.common.numeric import RoundRule as RoundRuleBase, AbsRule as AbsRuleBase

from .base import PolarsMixin


class RoundRule(RoundRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        return col.round(self.scale)


class AbsRule(AbsRuleBase, PolarsMixin):
    def do_apply(self, df, col):
        return col.abs()
