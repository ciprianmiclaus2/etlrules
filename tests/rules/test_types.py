import pytest

from etlrules.exceptions import MissingColumnError, UnsupportedTypeError
from tests.utils.data import assert_frame_equal, get_test_data


@pytest.mark.parametrize("input_df,input_astype,conversion_dict,expected,expected_info", [
    [[
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {'A': 'Int64'}, {'A': 'int8'}, [
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {"A": "Int8"}],
    [[
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {'A': 'Int64'}, {'A': 'int16'}, [
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {"A": "Int16"}],
    [[
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {'A': 'Int64'}, {'A': 'int32'}, [
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {"A": "Int32"}],
    [[
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {'A': 'Int64'}, {'A': 'int64'}, [
        {'A': 1},
        {'A': 2},
        {'A': 3},
        {'A': -1},
        {}
    ], {"A": "Int64"}],
    [[
        {'A': 1},
        {'A': 2},
        {'A': 3},
        #{'A': -1},
        {}
    ], {'A': 'Int64'}, {'A': 'uint8'}, [
        {'A': 1},
        {'A': 2},
        {'A': 3},
        #{'A': 255},
        {}
    ], {"A": "UInt8"}],
    [[
        {'A': 1},
        {'A': 2},
        {}
    ], {'A': 'UInt8'}, {'A': 'uint8'}, [
        {'A': 1},
        {'A': 2},
        {}
    ], {"A": "UInt8"}],
    [[
        {'A': 1},
        {'A': 2},
        {}
    ], {'A': 'UInt8'}, {'A': 'uint64'}, [
        {'A': 1},
        {'A': 2},
        {}
    ], {"A": "UInt64"}],
    [[
        {'A': '1', 'B': 2},
        {'A': '2', 'B': 3},
        {'A': '3', 'B': 4},
    ], None, {'A': 'int64', 'B': 'string'}, [
        {'A': 1, 'B': '2'},
        {'A': 2, 'B': '3'},
        {'A': 3, 'B': '4'},
    ], {"A": "Int64", "B": "string"}],
    [[
        {'A': '1.5', 'B': 2.0},
        {'A': '2.0', 'B': 3.45},
        {'A': '3.45', 'B': 4.5},
    ], None, {'A': 'float64', 'B': 'string'}, [
        {'A': 1.5, 'B': '2.0'},
        {'A': 2.0, 'B': '3.45'},
        {'A': 3.45, 'B': '4.5'},
    ], {"B": "string"}],
    [[
        {'A': 1, 'B': 2.0},
        {'A': 2, 'B': 3.0},
        {'A': 3, 'B': 4.0},
    ], None, {'A': 'float64', 'B': 'int64'}, [
        {'A': 1.0, 'B': 2},
        {'A': 2.0, 'B': 3},
        {'A': 3.0, 'B': 4},
    ], {"B": "Int64"}],
    [[
        {'A': 1.5},
        {'A': 2.7689},
        {}
    ], {'A': 'Float64'}, {'A': 'string'}, [
        {'A': "1.5"},
        {'A': "2.7689"},
        {}
    ], {"A": "string"}],
    [{"A": [], "B": []}, {"A": "string", "B": "int64"},
        {'A': 'int64', 'B': 'string'},
        {"A": [], "B": []}, {"A": "Int64", "B": "string"}],
    
    # exceptions
    [[{'A': '1', 'B': 2}, {'A': '2', 'B': 3}], None, {'A': 'int64', 'C': 'string'}, MissingColumnError, "Column 'C' is missing in the data frame"],
    [[{'A': '1', 'B': 2}, {'A': '2', 'B': 3}], None, {'A': 'int64', 'B': 'unknown'}, UnsupportedTypeError, "Type 'unknown' for column 'B' is not currently supported."],
    [[
        {'A': "2023-04-05 10:40:50"},
        {'A': "2023-05-04 09:40:50"},
        {}
    ], {'A': 'string'}, {'A': 'datetime'}, UnsupportedTypeError, "Type 'datetime' for column 'A' is not currently supported."],
])
def test_type_conversion_rule_scenarios(input_df, input_astype, conversion_dict, expected, expected_info, backend):
    input_df = backend.DataFrame(input_df, astype=input_astype)
    expected = backend.DataFrame(expected, astype=expected_info) if isinstance(expected, (list, dict)) else expected
    with get_test_data(input_df, named_inputs={"other": input_df}, named_output="result") as data:
        if isinstance(expected, backend.impl.DataFrame):
            rule = backend.rules.TypeConversionRule(conversion_dict, named_output="result")
            rule.apply(data)
            actual = data.get_named_output("result")
            assert_frame_equal(actual, expected)
        elif issubclass(expected, Exception):
            with pytest.raises(expected) as exc:
                rule = backend.rules.TypeConversionRule(conversion_dict, named_output="result")
                rule.apply(data)
            if expected_info:
                assert expected_info in str(exc.value)
        else:
            assert False
