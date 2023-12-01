import os
from pathlib import Path
import pytest
import sys
from unittest.mock import patch

from etlrules.runner import run
from etlrules.backends.common.io.db import SQLAlchemyEngines


EXPECTED = [
    (1, 'Mike', 'Geoffrey', 'Like a python', 2012),
    (2, 'Adam', 'Rolley', 'How to make friends', 1998),
    (3, 'Michelle', 'Saville', 'Scent of a pear', 2022),
    (4, 'Dorothy', 'Andrews', 'Tell me your name', 2014),
    (5, 'John', 'Rawley', 'Make me a lasagna', 2011),
]


@pytest.mark.parametrize("backend_str", [
    "pandas",
    "polars",
])
def test_runner(backend_str):
    args = ["runner.py", "-p", "./tests/csv2db.yml", "-b", backend_str]
    with patch.object(sys, 'argv', args):
        run()
    try:
        import sqlalchemy as sa
        engine = SQLAlchemyEngines.get_engine("sqlite:///tests/mydb.db")
        with engine.connect() as connection:
            rows = connection.execute(sa.text("SELECT * FROM MyTable")).fetchall()
            assert rows == EXPECTED
    finally:
        os.remove(Path("tests") / "mydb.db")


@pytest.mark.parametrize("backend_str", [
    "pandas",
    "polars",
])
def test_runner_with_cli_overrides(backend_str):
    db_name = "otherdb.db"
    args = [
        "runner.py", "-p", "./tests/csv2db.yml", "-b", backend_str,
        "--csv_file_dir", "./tests",
        "--csv_file_name", "csv_sample.csv",
        "--sql_engine", f"sqlite:///tests/{db_name}",
        "--sql_table", "SomeOtherTable"
    ]
    with patch.object(sys, 'argv', args):
        run()
    try:
        import sqlalchemy as sa
        engine = SQLAlchemyEngines.get_engine(f"sqlite:///tests/{db_name}")
        with engine.connect() as connection:
            rows = connection.execute(sa.text("SELECT * FROM SomeOtherTable")).fetchall()
            assert rows == EXPECTED
    finally:
        os.remove(Path("tests") / db_name)
