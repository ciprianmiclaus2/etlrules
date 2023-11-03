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

    def DataFrame(self, data, dtype=None, astype=None):
        if self.impl_pckg == pd:
            df = pd.DataFrame(data, dtype=dtype)
            if astype is not None:
                df = df.astype(astype)
        elif self.impl_pckg == pl:
            df = pl.DataFrame(data)
        else:
            assert False, f"unknown impl_pckg: {self.impl_pckg}"
        return df


@pytest.fixture(params=[('pandas', pd, pd_rules), ('polars', pl, pl_rules)])
def backend(request):
    return BackendFixture(*request.param)
