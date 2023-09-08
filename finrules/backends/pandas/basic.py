from pandas import DataFrame
from finrules.rule import BaseRule
from finrules.backends.common.basic import BaseStartRule
from .validation import PandasRuleValidationMixin


class StartRule(BaseStartRule, PandasRuleValidationMixin):
    ...
