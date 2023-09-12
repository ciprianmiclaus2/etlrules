from finrules.rule import BaseRule


class SortRule(BaseRule):
    def __init__(self, sort_by, ascending=True, named_input=None, named_output=None, strict=True):
        super().__init__(named_input=named_input, named_output=named_output, strict=strict)
        assert isinstance(sort_by, str) or (isinstance(sort_by, (list, tuple)) and all(isinstance(val, str) for val in sort_by)), "sort_by must be a str (single column) or a list of str (multiple columns)"
        self.sort_by = sort_by
        if isinstance(self.sort_by, str):
            self.sort_by = [self.sort_by]
        assert isinstance(ascending, bool) or (isinstance(ascending, (list, tuple)) and all(isinstance(val, bool) for val in ascending) and len(ascending) == len(self.sort_by)), "ascending must be a bool or a list of bool of the same len as sort_by"
        self.ascending = ascending

    def apply(self, data):
        df = self._get_input_df(data)
        df = df.sort_values(by=self.sort_by, ascending=self.ascending, ignore_index=True)
        self._set_output_df(data, df)
