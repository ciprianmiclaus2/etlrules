import pyarrow as pa
import pyarrow.compute as pc

from etlrules.backends.common.types import TypeConversionRule as TypeConversionRuleBase
from etlrules.backends.pyarrow.base import PyArrowMixin


MAP_TYPES = {
    'int8': pa.int8(),
    'int16': pa.int16(),
    'int32': pa.int32(),
    'int64': pa.int64(),
    'uint8': pa.uint8(),
    'uint16': pa.uint16(),
    'uint32': pa.uint32(),
    'uint64': pa.uint64(),
    'float32': pa.float32(),
    'float64': pa.float64(),
    'string': pa.string(),
    'boolean': pa.bool_(),
}

CAST_ERRORS = (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError)


def _cast_scalar_or_null(value: pa.Scalar, dtype: pa.DataType):
    if not value.is_valid:
        return None
    if pa.types.is_string(value.type) and (pa.types.is_integer(dtype) or pa.types.is_floating(dtype)):
        value = pa.scalar(value.as_py().strip())
    try:
        return value.cast(dtype).as_py()
    except CAST_ERRORS:
        return None


def cast(col, dtype: pa.DataType, strict: bool = True):
    """ Casts a column to dtype.

    In strict mode, a value which cannot be converted raises ValueError.
    In non-strict mode, the values which cannot be converted become nulls.
    """
    try:
        result = pc.cast(col, dtype)
        if pa.types.is_floating(col.type) and pa.types.is_string(dtype):
            # like python (and pandas): 2.0 -> "2.0" (arrow casts it to "2")
            integral = pc.match_substring_regex(result, r"^-?\d+$")
            result = pc.if_else(integral, pc.binary_join_element_wise(result, ".0", ""), result)
        return result
    except CAST_ERRORS as exc:
        if strict:
            raise ValueError(str(exc)) from exc
    # slower, value by value conversion
    chunks = col.chunks if isinstance(col, pa.ChunkedArray) else [col]
    return pa.chunked_array(
        [pa.array([_cast_scalar_or_null(value, dtype) for value in chunk], type=dtype) for chunk in chunks],
        type=dtype,
    )


class TypeConversionRule(TypeConversionRuleBase, PyArrowMixin):

    def do_type_conversion(self, df, col, dtype):
        return cast(col, MAP_TYPES[dtype], strict=self.strict)
