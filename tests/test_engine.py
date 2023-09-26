from pandas import DataFrame
from pandas.testing import assert_frame_equal

from etlrules.data import RuleData
from etlrules.engine import RuleEngine
from etlrules.plan import Plan
from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule


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
    plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}))
    errors = plan.validate()
    assert not errors
    rule_engine = RuleEngine(plan)
    rule_engine.run(data)
    result = data.get_main_output()
    expected = DataFrame(data=[
        {'AA': 1, 'BB': 'm'},
        {'AA': 2, 'BB': 'n'},
        {'AA': 3, 'BB': 'p'},
    ])
    assert_frame_equal(result, expected)
