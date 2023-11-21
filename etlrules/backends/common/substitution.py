import os
from urllib.parse import quote_plus


class OSEnvironSubst:
    def __init__(self, use_quote_plus=True):
        self.use_quote_plus = use_quote_plus

    def __getattr__(self, attr_name):
        val = os.environ.get(attr_name) or ""
        if self.use_quote_plus:
            val = quote_plus(val)
        return val
