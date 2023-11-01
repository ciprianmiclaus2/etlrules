import pytest
import pandas as pd
import polars as pl

from etlrules.backends import pandas as pd_rules
from etlrules.backends import polars as pl_rules


class BackendFixture:
    def __init__(self, name, impl_pckg, rules_pckgs):
        self.name = name
        self.impl_pckg = impl_pckg
        self.rules_pckgs = rules_pckgs

    @property
    def impl(self):
        return self.impl_pckg

    @property
    def rules(self):
        return self.rules_pckgs

    def __str__(self):
        return self.name


@pytest.fixture(params=[('pandas', pd, pd_rules), ('polars', pl, pl_rules)])
def backend(request):
    return BackendFixture(*request.param)
