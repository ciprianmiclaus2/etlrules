from pandas import DataFrame
from pandas.testing import assert_frame_equal
from finrules.data import RuleData
from finrules.backends.pandas.basic import StartRule


def test_start_rule_main_input():
    main_input = DataFrame(data=[{'A': 1, 'B': 'b'}])
    data = RuleData()
    rule = StartRule(main_input)
    rule.apply(data)
    assert_frame_equal(data.get_main_output(), main_input)


def test_start_rule_main_input_and_named_inputs():
    main_input = DataFrame(data=[{'A': 1, 'B': 'b'}])
    named1 = DataFrame(data=[{'C': 2, 'D': 'd'}])
    named2 = DataFrame(data=[{'D': 3, 'E': 'e'}])
    data = RuleData()
    rule = StartRule(main_input, named_inputs={'named1': named1, 'named2': named2})
    rule.apply(data)
    assert_frame_equal(data.get_main_output(), main_input)
    assert_frame_equal(data.get_named_output('named1'), named1)
    assert_frame_equal(data.get_named_output('named2'), named2)
