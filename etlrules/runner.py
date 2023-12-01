import argparse
import logging
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
        help="The backend to use for running the plan (e.g. pandas, polars).",
        choices=["pandas", "polars"],
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


def run_plan(plan_file: str, backend: str) -> RuleData:
    """ Runs a plan from a yaml file with a given backend.

    Basic usage:

        from etlrules import run_plan
        data = run_plan("/home/someuser/some_plan.yml", "pandas")

    Args:
        plan_file: A path to a yaml file with the plan definition
        backend: One of the supported backends (e.g. pandas, polars, etc.)

    Returns:
        A RuleData instance which contains the result dataframe(s).
    """
    plan = load_plan(plan_file, backend)
    args = get_args_parser(plan)
    data = RuleData(context=args)
    engine = RuleEngine(plan)
    engine.run(data)
    return data


def run() -> None:
    args = get_args_parser()
    logger.info(f"Running plan '{args['plan']}' with backend: {args['backend']}")
    run_plan(args["plan"], args["backend"])
    logger.info("Done.")


if __name__ == "__main__":
    run()
