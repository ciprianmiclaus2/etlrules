import argparse
import logging
import os
import shutil
import tempfile
from typing import Any, Mapping, Optional, Sequence, Union

from .data import RuleData
from .engine import RuleEngine
from .plan import Plan


logger = logging.getLogger(__name__)


def _str_to_bool(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in ("1", "true", "yes", "y", "on"):
        return True
    if lowered in ("0", "false", "no", "n", "off"):
        return False
    raise argparse.ArgumentTypeError(f"Expected a boolean value (true/false/yes/no/1/0), got '{value}'.")


def get_args_parser(plan: Optional[Plan]=None, args: Optional[Sequence[str]]=None) -> dict[str, Any]:
    """ Parses the command line arguments of the etlrules runner.

    Args:
        plan: An optional plan. When given, each key in the plan's context becomes an optional
            command line argument (--key value) which overrides the plan's context value.
        args: The arguments to parse. Defaults to sys.argv[1:].

    Returns:
        A dictionary with the parsed arguments.
    """
    parser = argparse.ArgumentParser(prog="etlrules", description="Run an etlrules plan from a yaml file.")
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
            # bool must be checked before int as bool is a subclass of int
            if isinstance(val, bool):
                val_type = _str_to_bool
            elif isinstance(val, int):
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
        parsed_args = parser.parse_args(args)
    else:
        parsed_args, _ = parser.parse_known_args(args)
    return vars(parsed_args)


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


def _run_plan(plan: Plan, context: Optional[Mapping[str, Union[str, int, float, bool]]]=None) -> RuleData:
    run_context = dict(context or {})
    etlrules_tempdir, etlrules_tempdir_cleanup = get_etlrules_temp_dir()
    run_context.update({
        "etlrules_tempdir": etlrules_tempdir,
        "etlrules_tempdir_cleanup": etlrules_tempdir_cleanup,
    })
    try:
        data = RuleData(context=run_context)
        engine = RuleEngine(plan)
        engine.run(data)
    finally:
        if etlrules_tempdir_cleanup:
            shutil.rmtree(etlrules_tempdir)

    return data


def run_plan(
    plan_file: str,
    backend: str,
    context: Optional[Mapping[str, Union[str, int, float, bool]]]=None
) -> RuleData:
    """ Runs a plan from a yaml file with a given backend.

    The backend referers to the underlying dataframe library used to run
    the plan.

    Basic usage:

        from etlrules import run_plan
        data = run_plan("/home/someuser/some_plan.yml", "pandas")

        # override some of the values in the plan's context
        data = run_plan("/home/someuser/some_plan.yml", "pandas", context={"csv_file_name": "other.csv"})

    Args:
        plan_file: A path to a yaml file with the plan definition
        backend: One of the supported backends
        context: An optional mapping of values which override the values in the plan's context.

    Note:
        The supported backends:
            pandas, polars, dask

    Note:
        Unlike the command line runner, run_plan doesn't read the command line arguments (sys.argv).

    Returns:
        A RuleData instance which contains the result dataframe(s).
    """
    plan = load_plan(plan_file, backend)
    return _run_plan(plan, context)


def run(args: Optional[Sequence[str]]=None) -> None:
    """ The command line entry point (the etlrules command or python -m etlrules.runner). """
    known_args = get_args_parser(args=args)
    plan = load_plan(known_args["plan"], known_args["backend"])
    # the plan's context values can be overridden from the command line (e.g. --csv_file_name other.csv)
    context = get_args_parser(plan, args=args)
    logger.info(f"Running plan '{context['plan']}' with backend: {context['backend']}")
    _run_plan(plan, context)
    logger.info("Done.")


if __name__ == "__main__":
    run()
