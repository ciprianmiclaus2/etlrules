import datetime
import os
import pytest
try:
    import sqlalchemy as sa
    HAS_SQL_ALCHEMY = True
except ImportError:
    HAS_SQL_ALCHEMY = False

from etlrules.data import context
from etlrules.exceptions import SQLError, UnsupportedTypeError
from etlrules.backends.common.io.db import SQLAlchemyEngines

from tests.utils.data import assert_frame_equal, get_test_data


@pytest.fixture
def sqlite3_db():
    import sqlite3
    file_name = f"test_etlrules_{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None):%Y_%m_%d_%H_%M_%s_%f}.db"
    con = sqlite3.connect(file_name)
    cur = con.cursor()
    cur.execute("CREATE TABLE Author (Id INTEGER, FirstName TEXT, LastName TEXT, PRIMARY KEY(Id))")
    cur.execute("INSERT INTO Author (Id, FirstName, LastName) VALUES (1, 'Mike', 'Good')")
    cur.execute("INSERT INTO Author (Id, FirstName, LastName) VALUES (2, 'John', 'McEwan')")
    con.commit()
    con.close()
    try:
        yield file_name
    finally:
        os.remove(file_name)


@pytest.mark.skipif(not HAS_SQL_ALCHEMY, reason="sqlalchemy not installed.")
@pytest.mark.parametrize("sql_query,column_types,expected,expected_info", [
    ["SELECT * FROM Author", {"Id": "int64", "FirstName": "string", "LastName": "string"}, [
        {"Id": 1, "FirstName": "Mike", "LastName": "Good"},
        {"Id": 2, "FirstName": "John", "LastName": "McEwan"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],
    ["SELECT * FROM Author WHERE Id=2", {"Id": "int64", "FirstName": "string", "LastName": "string"}, [
        {"Id": 2, "FirstName": "John", "LastName": "McEwan"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],
    ["SELECT * FROM Author WHERE {context.SQL_FILTER}", {"Id": "int64", "FirstName": "string", "LastName": "string"}, [
        {"Id": 2, "FirstName": "John", "LastName": "McEwan"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],
    ["SELECT * FROM Author WHERE Id=-1", {"Id": "int64", "FirstName": "string", "LastName": "string"}, 
    {"Id": [], "FirstName": [], "LastName": []},
    {"Id": "Int64", "FirstName": "string", "LastName": "string"}],

    # exceptions
    ["SELECT * FROM Author WHERE Id=-1", {"Id": "int64", "FirstName": "string", "LastName": "unknown"},
    UnsupportedTypeError,
    "Type 'unknown' for column 'LastName' is not supported."],
    ["", None,
    ValueError,
    "The sql_query parameter must be a non-empty string."],
    ["SELECT Author WHERE Id=-1", None,
    SQLError,
    "no such column: Author"],
])
def test_read_sql_query_scenarios(sql_query, column_types, expected, expected_info, sqlite3_db, backend):
    expected = backend.DataFrame(expected, astype=expected_info) if isinstance(expected, (list, dict)) else expected
    with context.set({"SQL_FILTER": "Id=2"}):
        with get_test_data(None, named_inputs={}, named_output="result") as data:
            if isinstance(expected, backend.impl.DataFrame):
                rule = backend.rules.ReadSQLQueryRule(f"sqlite:///{sqlite3_db}", sql_query, column_types=column_types, named_output="result")
                rule.apply(data)
                actual = data.get_named_output("result")
                assert_frame_equal(actual, expected)
            elif issubclass(expected, Exception):
                with pytest.raises(expected) as exc:
                    rule = backend.rules.ReadSQLQueryRule(f"sqlite:///{sqlite3_db}", sql_query, column_types=column_types, named_output="result")
                    rule.apply(data)
                if expected_info:
                    assert expected_info in str(exc.value)
            else:
                assert False


@pytest.mark.skipif(not HAS_SQL_ALCHEMY, reason="sqlalchemy not installed.")
@pytest.mark.parametrize("input_df, input_astypes, sql_table, if_exists, expected, expected_info", [

    # new table - replace
    [[
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}, "Author2", "replace", [
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],

    # new table - append
    [[
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}, "Author2", "append", [
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],

    # new table - fail
    [[
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}, "Author2", "fail", [
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],

    # existing table - replace
    [[
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}, "Author", "replace", [
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],

    # existing table - append
    [[
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}, "Author", "append", [
        {"Id": 1, "FirstName": "Mike", "LastName": "Good"},
        {"Id": 2, "FirstName": "John", "LastName": "McEwan"},
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}],

    # existing table - fail
    [[
        {"Id": 10, "FirstName": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "LastName": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}, "Author", "fail",
    ValueError, "Table 'Author' already exists."],

    # existing table - append - incorrect schema
    [[
        {"Id": 10, "Bla": "Layla", "LastName": "Goodge"},
        {"Id": 11, "FirstName": "Craig", "Bla": "David"},
    ], {"Id": "Int64", "FirstName": "string", "LastName": "string"}, "Author", "append",
    SQLError, "table Author has no column named Bla"],
])
def test_write_sql_table_scenarios(input_df, input_astypes, sql_table, if_exists, expected, expected_info, sqlite3_db, backend):
    input_df = backend.DataFrame(input_df, astype=input_astypes) if isinstance(input_df, (list, dict)) else input_df
    with get_test_data(named_inputs={"input": input_df}, named_output="result") as data:
        if isinstance(expected, (list, dict)):
            rule = backend.rules.WriteSQLTableRule(f"sqlite:///{sqlite3_db}", sql_table, if_exists=if_exists, named_input="input")
            rule.apply(data)
            engine = SQLAlchemyEngines.get_engine(f"sqlite:///{sqlite3_db}")
            with engine.connect() as connection:
                res = connection.execute(sa.text(f"SELECT * FROM {sql_table}"))
                actual = [dict(zip(res.keys(), row)) for row in res]
            assert actual == expected
        elif issubclass(expected, Exception):
            with pytest.raises(expected) as exc:
                rule = backend.rules.WriteSQLTableRule(f"sqlite:///{sqlite3_db}", sql_table, if_exists=if_exists, named_input="input")
                rule.apply(data)
            if expected_info:
                assert expected_info in str(exc.value)
        else:
            assert False


def test_sql_engine_env_substitution(backend):
    user = os.environ.get("DB_USER")
    pswd = os.environ.get("DB_PASSWORD")
    os.environ["DB_USER"] = "testUser"
    os.environ["DB_PASSWORD"] = "testPassword"
    try:
        # DISCLAIMER - not a valid sqlite engine string
        rule = backend.rules.ReadSQLQueryRule("sqlite:///user={env.DB_USER}&pswd={env.DB_PASSWORD}", "SELECT * FROM MyTable", named_output="result")
        assert rule._get_sql_engine() == "sqlite:///user=testUser&pswd=testPassword"
        rule = backend.rules.ReadSQLQueryRule("sqlite:///user={env.DB_USER}", f"SELECT * FROM MyTable", named_output="result")
        assert rule._get_sql_engine() == "sqlite:///user=testUser"
    finally:
        if user:
            os.environ["DB_USER"] = user
        else:
            del os.environ["DB_USER"]
        if pswd:
            os.environ["DB_PASSWORD"] = pswd
        else:
            del os.environ["DB_PASSWORD"]


def test_sql_engine_context_substitution(backend):
    with context.set({"DB_USER": "testUser", "DB_PASSWORD": "testPassword"}):
        # DISCLAIMER - not a valid sqlite engine string
        rule = backend.rules.ReadSQLQueryRule("sqlite:///user={context.DB_USER}&pswd={context.DB_PASSWORD}", "SELECT * FROM MyTable", named_output="result")
        assert rule._get_sql_engine() == "sqlite:///user=testUser&pswd=testPassword"
        rule = backend.rules.ReadSQLQueryRule("sqlite:///user={context.DB_USER}", f"SELECT * FROM MyTable", named_output="result")
        assert rule._get_sql_engine() == "sqlite:///user=testUser"


def test_read_sql_query_nullable_ints_in_batches(backend, tmp_path):
    # integer columns with nulls are read as nullable ints (not floats) by all the backends, also when the data
    # is read in batches where some batches have only nulls; the order of the rows is kept (more than 10 batches)
    import sqlite3
    import pandas as pd
    db_file = tmp_path / "nullable.db"
    values = [None, 1, None, 3, 4, None, 6, 7, 8, 9, None, 11]
    with sqlite3.connect(db_file) as conn:
        conn.execute("CREATE TABLE T (Id INTEGER, Value INTEGER)")
        conn.executemany("INSERT INTO T (Id, Value) VALUES (?, ?)", list(enumerate(values)))
    with get_test_data(None, named_inputs={}, named_output="result") as data:
        rule = backend.rules.ReadSQLQueryRule(f"sqlite:///{db_file}", "SELECT * FROM T ORDER BY Id", batch_size=1, named_output="result")
        rule.apply(data)
        result = data.get_named_output("result")
        if backend.name == "dask":
            result = result.compute()
        if backend.name == "polars":
            assert result["Value"].dtype == backend.impl.Int64
            assert result["Value"].to_list() == values
        else:
            assert str(result["Value"].dtype) == "Int64"
            assert [None if v is pd.NA else v for v in result["Value"].tolist()] == values
        assert list(result["Id"]) == list(range(len(values)))
