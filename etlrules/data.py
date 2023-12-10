from contextlib import contextmanager
from typing import Generator, Mapping, Optional, Union


class RuleData:
    def __init__(self,
        main_input=None,
        named_inputs=None,
        context: Optional[Mapping[str, Union[str, int, float, bool]]]=None,
        strict: bool=True
    ):
        self.strict = strict
        self.main_output = main_input
        self.named_outputs = (
            {name: df for name, df in named_inputs.items()} if named_inputs else {}
        )
        self.context = {k: v for k, v in context.items()} if context is not None else {}
        self.lineage_info = {}

    def get_main_output(self):
        return self.main_output

    def set_main_output(self, df):
        self.main_output = df

    def get_named_output(self, name: str):
        assert name in self.named_outputs, f"No such named output {name}"
        return self.named_outputs[name]

    def set_named_output(self, name, df):
        if self.strict:
            assert (
                name not in self.named_outputs
            ), f"{name} already exists as a named output. It will be overwritten."
        self.named_outputs[name] = df

    def get_named_outputs(self):
        yield from self.named_outputs.items()

    def get_context(self) -> dict[str, Union[str, int, float, bool]]:
        return self.context


class Context:

    def __init__(self):
        self.mappers = []

    @contextmanager
    def set(self, mapping: Mapping[str, Union[str, int, float, bool]]) -> Generator[dict[str, str], None, None]:
        self.mappers.append(
            {k: v for k, v in mapping.items()}
        )
        try:
            yield self.mappers[-1]
        finally:
            self.mappers.pop()

    def _do_get_attr(self, attr_name: str) -> Union[str, int, float, bool]:
        if not isinstance(attr_name, str):
            raise TypeError("Context attr name must be a string.")
        if not self.mappers:
            raise RuntimeError("No context set.")
        for current_context in reversed(self.mappers):
            try:
                return current_context[attr_name]
            except KeyError:
                ...
        raise KeyError(f"No such attribute '{attr_name}' found in the current context.")


    def __getattr__(self, attr_name: str) -> Union[str, int, float, bool]:
        return self._do_get_attr(attr_name)

    def __getitem__(self, attr_name: str) -> Union[str, int, float, bool]:
        return self._do_get_attr(attr_name)


context = Context()
