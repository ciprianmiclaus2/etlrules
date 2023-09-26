
from contextlib import contextmanager
from copy import deepcopy
from pandas.testing import assert_frame_equal

from etlrules.data import RuleData


@contextmanager
def get_test_data(main_input=None, named_inputs=None, named_output=None, strict=True):
    data = TestRule(main_input=main_input, named_inputs=named_inputs, named_output=named_output, strict=strict)
    yield data
    data.validate()


class TestRule(RuleData):

    def __init__(self, main_input=None, named_inputs=None, named_output=None, strict=True):
        self.main_input_copy = deepcopy(main_input) if main_input is not None else None
        self.named_inputs_copies = {
            name: deepcopy(df) for name, df in (named_inputs or {}).items()
        }
        super().__init__(main_input=main_input, named_inputs=named_inputs, strict=strict)
        self.named_output = named_output

    def validate(self):
        if self.named_output is not None and self.main_input_copy is not None:
            assert_frame_equal(self.main_input_copy, self.get_main_output())
        for name, df in self.named_inputs_copies.items():
            if name != self.named_output:
                assert_frame_equal(df, self.get_named_output(name))
