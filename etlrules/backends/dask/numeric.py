from etlrules.backends.common.numeric import RoundRule as RoundRuleBase, AbsRule as AbsRuleBase

from .base import DaskMixin


class RoundRule(RoundRuleBase, DaskMixin):
    def do_apply(self, df, col):
        return col.round(self.scale)


class AbsRule(AbsRuleBase, DaskMixin):
    def do_apply(self, df, col):
        return col.abs()
