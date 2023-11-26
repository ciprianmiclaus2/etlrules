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
    with open(plan_file, 'rt') as plan_f:
        contents = plan_f.read()
    return Plan.from_yaml(contents, backend)


def run_plan() -> None:
    args = get_args_parser()
    plan = load_plan(args["plan"], args["backend"])
    args = get_args_parser(plan)
    data = RuleData(context=args)
    engine = RuleEngine(plan)
    logger.info(f"Running plan '{plan.name}'")
    engine.run(data)
    logger.info("Done.")


if __name__ == "__main__":
    run_plan()
