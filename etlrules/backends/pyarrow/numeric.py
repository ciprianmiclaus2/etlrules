import pyarrow.compute as pc

from etlrules.backends.common.numeric import RoundRule as RoundRuleBase, AbsRule as AbsRuleBase
from etlrules.backends.pyarrow.base import PyArrowMixin


class RoundRule(RoundRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return pc.round(col, ndigits=self.scale, round_mode="half_to_even")


class AbsRule(AbsRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return pc.abs_checked(col)
