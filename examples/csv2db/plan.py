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
        name="CSV2DB",
        description="""Reads a CSV file and writes it into a DB table.
        
        To generate a yaml file from this plan, run:
            python -m examples.csv2db.plan
        This should produce csv2db.yml in the curent directory.
        To run the plan, run:
            python -m etlrules.runner -p ./examples/csv2db/csv2db.yml -b pandas
        """,
        context={
            "csv_file_name": "csv_sample.csv",
            "csv_file_dir": "./examples/csv2db",

            "sql_engine": "sqlite:///examples/csv2db/mydb.db",
            "sql_table": "MyTable",
        },
        strict=True
    )

    backend_module = get_backend(backend)
    plan.add_rule(
        backend_module.ReadCSVFileRule(
            file_name="{context.csv_file_name}",
            file_dir="{context.csv_file_dir}",
            name="Load a csv file",
            description="""The rule can be customized by specifying:

            context.csv_file_name: The name of the csv to read
            context.csv_file_dir: The directory of the csv file

            When running in the runner, you can override the context params with:

            python -m etlrules.runner -p csv2db.yml -b pandas --csv_file_name another_file.csv --csv_file_dir /another/file/dir
            
            When not overridden, the defaults read the local csv_sample.csv file.
            """
        )
    )
    plan.add_rule(
        backend_module.WriteSQLTableRule(
            sql_engine="{context.sql_engine}",
            sql_table="{context.sql_table}",
            if_exists="append",
            name="Write the dataframe to the DB table",
            description="""The rule can be customized by specifying:

            context.sql_engine: The sql engine to write to
            context.sql_table: The sql table to write to

            If the table already exists, append to it.

            When running in the runner, you can override the context params with:

            python -m etlrules.runner -p csv2db.yml -b pandas --sql_engine some_sql_engine --sql_table SomeTable
            
            When not overridden, the defaults write to a sqlite3 local file called mydb.db into a table MyTable.
            """
        )
    )

    return plan


if __name__ == "__main__":
    plan = get_plan(backend="pandas")
    plan_yml = plan.to_yaml()
    output_file_name = Path(os.path.dirname(os.path.realpath(__file__))) / "csv2db.yml"
    with open(output_file_name, "wt") as f:
        f.write(plan_yml)
    print(f"Writen {output_file_name}")
