import pytest
import pandas as pd
import polars as pl


@pytest.fixture(params=[pd, pl])
def backend(request):
    return request.param
