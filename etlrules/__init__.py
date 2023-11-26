"""Top-level package for ETLrules."""

from .data import RuleData, context
from .engine import RuleEngine
from .exceptions import (
    ColumnAlreadyExistsError, ExpressionSyntaxError, GraphRuntimeError,
    InvalidPlanError, MissingColumnError, SchemaError, UnsupportedTypeError,
)
from .plan import Plan, PlanMode


__author__ = """Ciprian Miclaus"""
__email__ = "ciprianm@gmail.com"
__version__ = "0.2.2"


__all__ = [
    "RuleData", "context",
    "RuleEngine",
    "ColumnAlreadyExistsError", "ExpressionSyntaxError", "GraphRuntimeError",
    "InvalidPlanError", "MissingColumnError", "SchemaError", "UnsupportedTypeError",
    "Plan", "PlanMode",
]
