import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.numeric import RoundRule as RoundRuleBase, AbsRule as AbsRuleBase, ClipRule as ClipRuleBase
from etlrules.backends.pyarrow.base import PyArrowMixin


class RoundRule(RoundRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return pc.round(col, ndigits=self.scale, round_mode="half_to_even")


class AbsRule(AbsRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        return pc.abs_checked(col)


def _bound(value, col):
    # the bound with the type of the column when possible (e.g. 0 for a float column is 0.0)
    try:
        return pa.scalar(value, type=col.type)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return pa.scalar(value)


class ClipRule(ClipRuleBase, PyArrowMixin):

    def do_apply(self, df, col):
        # skip_nulls=False keeps the missing values missing (instead of replacing them with the bound)
        if self.lower is not None:
            col = pc.max_element_wise(col, _bound(self.lower, col), skip_nulls=False)
        if self.upper is not None:
            col = pc.min_element_wise(col, _bound(self.upper, col), skip_nulls=False)
        return col
