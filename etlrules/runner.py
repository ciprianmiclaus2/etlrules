import argparse
import logging
import os
import shutil
import tempfile
from typing import Any, Optional

from .data import RuleData
from .engine import RuleEngine
from .plan import Plan


logger = logging.getLogger(__name__)


def get_args_parser(plan: Optional[Plan]=None) -> dict[str, Any]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--plan",
        help='Specify a yaml file containing a plan to run.',
        required=True,
    )
    parser.add_argument(
        "-b",
        "--backend",
        help="The backend to use for running the plan.",
        choices=["pandas", "polars", "dask"],
        required=False,
        default="pandas"
    )
    if plan:
        context = plan.get_context()
        for key, val in context.items():
            if isinstance(val, int):
                val_type = int
            elif isinstance(val, float):
                val_type = float
            else:
                val_type = str
            parser.add_argument(
                "--" + key,
                required=False,
                default=val,
                type=val_type
            )
        args = parser.parse_args()
    else:
        args, _ = parser.parse_known_args()
    return vars(args)


def load_plan(plan_file: str, backend: str) -> Plan:
    """ Load a plan from a yaml file.

    Basic usage:

        from etlrules import load_plan
        plan = load_plan("/home/someuser/some_plan.yml", "pandas")

    Args:
        plan_file: A path to a yaml file with the plan definition
        backend: One of the supported backends (e.g. pandas, polars, etc.)

    Returns:
        A Plan instance deserialized from the yaml file.
    """
    with open(plan_file, 'rt') as plan_f:
        contents = plan_f.read()
    return Plan.from_yaml(contents, backend)


def get_etlrules_temp_dir() -> tuple[str, bool]:
    etlrules_tempdir_cleanup = True
    etlrules_tempdir = os.environ.get("ETLRULES_TEMPDIR")
    if etlrules_tempdir:
        if os.path.exists(etlrules_tempdir) and os.path.isdir(etlrules_tempdir):
            etlrules_tempdir_cleanup = False
        else:
            os.makedirs(etlrules_tempdir)
    else:
        etlrules_tempdir = tempfile.mkdtemp(prefix='tmp_etlrules')
    return etlrules_tempdir, etlrules_tempdir_cleanup


def run_plan(plan_file: str, backend: str) -> RuleData:
    """ Runs a plan from a yaml file with a given backend.

    The backend referers to the underlying dataframe library used to run
    the plan.

    Basic usage:

        from etlrules import run_plan
        data = run_plan("/home/someuser/some_plan.yml", "pandas")

    Args:
        plan_file: A path to a yaml file with the plan definition
        backend: One of the supported backends

    Note:
        The supported backends:
            pandas, polars, dask (work in progress)

    Returns:
        A RuleData instance which contains the result dataframe(s).
    """
    plan = load_plan(plan_file, backend)
    args = get_args_parser(plan)
    context = {}
    context.update(args)
    etlrules_tempdir, etlrules_tempdir_cleanup = get_etlrules_temp_dir()
    context.update({
        "etlrules_tempdir": etlrules_tempdir,
        "etlrules_tempdir_cleanup": etlrules_tempdir_cleanup,
    })
    try:
        data = RuleData(context=context)
        engine = RuleEngine(plan)
        engine.run(data)
    finally:
        if etlrules_tempdir_cleanup:
            shutil.rmtree(etlrules_tempdir)

    return data


def run() -> None:
    args = get_args_parser()
    logger.info(f"Running plan '{args['plan']}' with backend: {args['backend']}")
    run_plan(args["plan"], args["backend"])
    logger.info("Done.")


if __name__ == "__main__":
    run()
