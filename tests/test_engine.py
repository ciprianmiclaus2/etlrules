from pandas import DataFrame
from pandas.testing import assert_frame_equal

from finrules.data import RuleData
from finrules.engine import RuleEngine
from finrules.plan import Plan
from finrules.backends.pandas import ProjectRule, SortRule


def test_run_simple_plan():
    input_df = DataFrame(data=[
        {'A': 2, 'B': 'n', 'C': True},
        {'A': 1, 'B': 'm', 'C': False},
        {'A': 3, 'B': 'p', 'C': True},
    ])
    data = RuleData(input_df)
    plan = Plan()
    plan.add_rule(SortRule(['A']))
    plan.add_rule(ProjectRule(['A', 'B']))
    rule_engine = RuleEngine(plan)
    rule_engine.run(data)
    result = data.get_main_output()
    expected = DataFrame(data=[
        {'A': 1, 'B': 'm'},
        {'A': 2, 'B': 'n'},
        {'A': 3, 'B': 'p'},
    ])
    assert_frame_equal(result, expected)
