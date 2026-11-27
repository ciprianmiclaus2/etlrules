import os
from pathlib import Path
import pytest
import sys
from unittest.mock import patch

from etlrules.runner import run
from etlrules.backends.common.io.db import SQLAlchemyEngines


def _dispose_engine(sql_engine):
    # the engines (and their pooled connections) are cached: close them before the db file is deleted,
    # otherwise the next test reuses a connection to the deleted file
    engine = SQLAlchemyEngines.ENGINES.pop(sql_engine, None)
    if engine is not None:
        engine.dispose()


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
    "pyarrow",
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
        _dispose_engine("sqlite:///tests/mydb.db")
        os.remove(Path("tests") / "mydb.db")


@pytest.mark.parametrize("backend_str", [
    "pandas",
    "polars",
    "pyarrow",
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
        _dispose_engine(f"sqlite:///tests/{db_name}")
        os.remove(Path("tests") / db_name)


@pytest.mark.parametrize("backend_str", [
    "pandas",
    "polars",
    "dask",
    "pyarrow",
])
def test_run_plan_programmatic_ignores_command_line(backend_str, tmp_path):
    # run_plan is a library function: it must not parse sys.argv (which belongs to the caller's program)
    from etlrules import run_plan
    db_file = tmp_path / "progdb.db"
    with patch.object(sys, 'argv', ["some_other_program.py", "--unrelated", "arg"]):
        data = run_plan("./tests/csv2db.yml", backend_str, context={
            "sql_engine": f"sqlite:///{db_file}",
            "sql_table": "ProgTable",
        })
    assert data.get_context()["sql_table"] == "ProgTable"
    import sqlalchemy as sa
    engine = SQLAlchemyEngines.get_engine(f"sqlite:///{db_file}")
    with engine.connect() as connection:
        rows = connection.execute(sa.text("SELECT * FROM ProgTable")).fetchall()
    assert rows == EXPECTED


def test_get_args_parser_context_types():
    from etlrules.plan import Plan
    from etlrules.runner import get_args_parser
    plan = Plan(context={"flag": False, "other_flag": True, "count": 1, "ratio": 0.5, "name": "x"})
    args = get_args_parser(plan, args=[
        "-p", "plan.yml", "--flag", "true", "--other_flag", "no", "--count", "3", "--ratio", "1.5",
    ])
    assert args["flag"] is True
    assert args["other_flag"] is False
    assert args["count"] == 3
    assert args["ratio"] == 1.5
    assert args["name"] == "x"
    with pytest.raises(SystemExit):
        get_args_parser(plan, args=["-p", "plan.yml", "--flag", "maybe"])


def test_run_plan_temp_dir_from_environment(tmp_path, monkeypatch):
    # an existing ETLRULES_TEMPDIR is used and not deleted after the run (it may have other files in it)
    from etlrules import run_plan
    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "keep.txt").write_text("not etlrules'")
    db_file = tmp_path / "out.db"
    context = {"sql_engine": f"sqlite:///{db_file}"}
    monkeypatch.setenv("ETLRULES_TEMPDIR", str(existing))
    data = run_plan("./tests/csv2db.yml", "pandas", context=context)
    assert data.get_context()["etlrules_tempdir"] == str(existing)
    assert (existing / "keep.txt").exists()
    # a directory which doesn't exist is created for the run and deleted after it
    new_dir = tmp_path / "new"
    monkeypatch.setenv("ETLRULES_TEMPDIR", str(new_dir))
    data = run_plan("./tests/csv2db.yml", "pandas", context={**context, "sql_table": "Other"})
    assert data.get_context()["etlrules_tempdir"] == str(new_dir)
    assert not new_dir.exists()
    _dispose_engine(f"sqlite:///{db_file}")
