from pandas import DataFrame
from pandas.testing import assert_frame_equal
from finrules.data import RuleData
from finrules.backends.pandas.basic import StartRule


def test_start_rule_main_input():
    main_input = DataFrame()
    data = RuleData()
    rule = StartRule(main_input)
    rule.apply(data)
    assert_frame_equal(data.get_main_output(), main_input)
