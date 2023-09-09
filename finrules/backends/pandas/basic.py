from pandas import DataFrame

from finrules.backends.common.basic import BaseStartRule
from finrules.rule import BaseRule

from .validation import PandasRuleValidationMixin


class StartRule(BaseStartRule, PandasRuleValidationMixin):
    ...
