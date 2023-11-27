from types import ModuleType
import importlib
import os
from pathlib import Path

from etlrules.plan import Plan


def get_backend(backend: str) -> ModuleType:
    assert backend in ("pandas", "polars")
    return importlib.import_module("etlrules.backends." + backend)


def get_plan(backend) -> Plan:
    plan = Plan(
        mode="pipeline",
        name="DB2CSV",
        description="""Runs a DB query and writes the results to a local csv file.
        
        To generate a yaml file from this plan, run:
            python -m examples.db2csv.plan
        This should produce db2csv.yml in the curent directory.
        To run the plan, run:
            python -m etlrules.runner -p ./examples/db2csv/db2csv.yml -b pandas
        """,
        context={
            "sql_engine": "sqlite:///examples/db2csv/mydb.db",
            "sql_query": "SELECT * FROM MyTable",

            "csv_file_name": "csv_sample.csv",
            "csv_file_dir": "./examples/db2csv",
        },
        strict=True
    )

    backend_module = get_backend(backend)
    plan.add_rule(
        backend_module.ReadSQLQueryRule(
            sql_engine="{context.sql_engine}",
            sql_query="{context.sql_query}",
            name="Read a DB table",
            description="""The rule can be customized by specifying:

            context.sql_engine: The sql engine to read from
            context.sql_query: The sql query to run

            When running in the runner, you can override the context params with:

            python -m etlrules.runner -p db2csv.yml -b pandas --sql_engine some_sql_engine --sql_query "SELECT * FROM OtherTable"
            
            When not overridden, the defaults read from a sqlite3 local file called mydb.db and selects all records from a table MyTable.
            """
        )
    )
    plan.add_rule(
        backend_module.WriteCSVFileRule(
            file_name="{context.csv_file_name}",
            file_dir="{context.csv_file_dir}",
            name="Writes a local csv file.",
            description="""The rule can be customized by specifying:

            context.csv_file_name: The name of the csv to write to
            context.csv_file_dir: The directory to write the csv file into

            When running in the runner, you can override the context params with:

            python -m etlrules.runner -p db2csv.yml -b pandas --csv_file_name another_file.csv --csv_file_dir /another/file/dir
            
            When not overridden, the defaults write a local csv_sample.csv file.
            """
        )
    )

    return plan


if __name__ == "__main__":
    plan = get_plan(backend="pandas")
    plan_yml = plan.to_yaml()
    output_file_name = Path(os.path.dirname(os.path.realpath(__file__))) / "db2csv.yml"
    with open(output_file_name, "wt") as f:
        f.write(plan_yml)
    print(f"Writen {output_file_name}")
