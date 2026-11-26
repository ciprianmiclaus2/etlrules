""" Evaluation of the python expressions used by AddNewColumnRule, IfThenElseRule and FilterRule.

Expressions refer to columns as df['COLUMN']. pyarrow arrays don't support python operators, so df['COLUMN']
returns a Column wrapper which maps the python operators (+, -, *, /, %, **, comparisons, &, |, ^, ~, <<, >>)
to the pyarrow compute functions. When an expression cannot be evaluated on whole columns (e.g. it uses `and`,
`or`, `if`, or mixes types the compute functions don't support), it falls back to a slower row by row evaluation
where df['COLUMN'] is the python value of the column in the current row.
"""
import logging
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.expressions import Expression as ExpressionBase
from etlrules.backends.pyarrow.base import to_column
from etlrules.data import context

perf_logger = logging.getLogger("etlrules.perf")


def _type(value) -> pa.DataType:
    if isinstance(value, (pa.Array, pa.ChunkedArray, pa.Scalar)):
        return value.type
    return pa.scalar(value).type


def _is_string(value) -> bool:
    typ = _type(value)
    return pa.types.is_string(typ) or pa.types.is_large_string(typ)


def _is_integer(value) -> bool:
    return pa.types.is_integer(_type(value))


def _python_modulo(a, b):
    # python modulo semantics (the result has the sign of the divisor), for ints and floats
    if _is_integer(a) and _is_integer(b):
        quotient = pc.divide(a, b)  # truncated towards zero
        # floor the quotient when the operands have different signs and the division is not exact
        remainder = pc.subtract(a, pc.multiply(quotient, b))
        adjust = pc.and_(pc.not_equal(remainder, 0), pc.not_equal(pc.less(a, 0), pc.less(b, 0)))
        quotient = pc.if_else(adjust, pc.subtract(quotient, 1), quotient)
        return pc.subtract(a, pc.multiply(quotient, b))
    quotient = pc.floor(pc.divide(pc.cast(a, pa.float64()), pc.cast(b, pa.float64())))
    return pc.subtract(a, pc.multiply(quotient, b))


def _true_divide(a, b):
    if _is_integer(a):
        a = pc.cast(a, pa.float64())
    if _is_integer(b):
        b = pc.cast(b, pa.float64())
    return pc.divide(a, b)


def _add(a, b):
    if _is_string(a) or _is_string(b):
        if not (_is_string(a) and _is_string(b)):
            raise TypeError(f"Cannot add {_type(a)} and {_type(b)}")
        return pc.binary_join_element_wise(a, b, "")
    return pc.add_checked(a, b)


def _logical(boolean_kernel, bitwise_kernel):
    def op(a, b):
        if pa.types.is_boolean(_type(a)) and pa.types.is_boolean(_type(b)):
            return getattr(pc, boolean_kernel)(a, b)
        return getattr(pc, bitwise_kernel)(a, b)
    return op


BINARY_OPERATORS = {
    "add": _add,
    "sub": pc.subtract_checked,
    "mul": pc.multiply_checked,
    "truediv": _true_divide,
    "mod": _python_modulo,
    "pow": pc.power,
    "and": _logical("and_kleene", "bit_wise_and"),
    "or": _logical("or_kleene", "bit_wise_or"),
    "xor": _logical("xor", "bit_wise_xor"),
    "lshift": pc.shift_left,
    "rshift": pc.shift_right,
}

COMPARISONS = {
    "eq": pc.equal,
    "ne": pc.not_equal,
    "lt": pc.less,
    "le": pc.less_equal,
    "gt": pc.greater,
    "ge": pc.greater_equal,
}


class Column:
    """ A column in an expression, supporting python operators. """

    def __init__(self, data):
        self.data = data

    def __bool__(self):
        # and, or, not, if ... else need a single truth value: let the expression be evaluated row by row
        raise ValueError("The truth value of a column is ambiguous.")

    def __invert__(self):
        if pa.types.is_boolean(self.data.type):
            return Column(pc.invert(self.data))
        return Column(pc.bit_wise_not(self.data))

    def __neg__(self):
        return Column(pc.negate_checked(self.data))

    def __abs__(self):
        return Column(pc.abs_checked(self.data))

    __hash__ = None


def _unwrap(value):
    return value.data if isinstance(value, Column) else value


def _make_binary(func):
    def method(self, other):
        return Column(func(self.data, _unwrap(other)))

    def rmethod(self, other):
        return Column(func(_unwrap(other), self.data))
    return method, rmethod


for _name, _func in BINARY_OPERATORS.items():
    _method, _rmethod = _make_binary(_func)
    setattr(Column, f"__{_name}__", _method)
    setattr(Column, f"__r{_name}__", _rmethod)

for _name, _func in COMPARISONS.items():
    setattr(Column, f"__{_name}__", _make_binary(_func)[0])


class Columns:
    """ The df in an expression: df['COLUMN'] returns the column. """

    def __init__(self, table: pa.Table):
        self.table = table

    def __getitem__(self, name):
        if name not in self.table.column_names:
            raise KeyError(name)
        return Column(self.table[name])


FALLBACK_ERRORS = (TypeError, ValueError, pa.ArrowNotImplementedError)


class Expression(ExpressionBase):

    def _eval_rows(self, table: pa.Table):
        rows = table.to_pylist()
        return [eval(self._compiled_expr, {}, {'df': row, 'context': context}) for row in rows]

    def eval(self, df: pa.Table) -> Any:
        """ Evaluates the expression on the table.

        Returns:
            A chunked array with the results (one per row) or a scalar (python value).
        """
        try:
            result = eval(self._compiled_expr, {}, {'df': Columns(df), 'context': context})
            if isinstance(result, Column):
                return result.data
            return result
        except FALLBACK_ERRORS:
            if df.num_rows == 0:
                return pa.chunked_array([pa.array([], type=pa.string())])
            perf_logger.warning("Evaluating expression '%s' is not vectorized and might hurt the overall performance.", self.expression_str)
            values = self._eval_rows(df)
            try:
                return pa.chunked_array([pa.array(values)])
            except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
                raise TypeError(f"The expression '{self.expression_str}' produced values of different types: {exc}") from exc


def evaluate_to_column(expression: Expression, df: pa.Table) -> pa.ChunkedArray:
    return to_column(expression.eval(df), df.num_rows)
