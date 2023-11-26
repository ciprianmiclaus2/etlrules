import os
from urllib.parse import quote_plus

from etlrules.data import context


class OSEnvironSubst:
    def __init__(self, use_quote_plus=True):
        self.use_quote_plus = use_quote_plus

    def __getattr__(self, attr_name):
        val = os.environ.get(attr_name) or ""
        if self.use_quote_plus:
            val = quote_plus(val)
        return val


def subst_string(str_in: str) -> str:
    return str_in.format(
        env=OSEnvironSubst(),
        context=context
    )
