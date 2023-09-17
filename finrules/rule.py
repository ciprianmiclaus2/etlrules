import importlib
import yaml

from .data import RuleData


class BaseRule:
    def __init__(self, named_input=None, named_output=None, name=None, description=None, strict=True):
        assert named_input is None or isinstance(named_input, str) and named_input
        assert named_output is None or isinstance(named_output, str) and named_output
        self.named_input = named_input
        self.named_output = named_output
        self.name = name
        self.description = description
        self.strict = strict

    def rule_name(self):
        return self.name

    def rule_description(self):
        return self.description

    def _get_input_df(self, data):
        if self.named_input is None:
            return data.get_main_output()
        return data.get_named_output(self.named_input)

    def _set_output_df(self, data, df):
        if self.named_output is None:
            data.set_main_output(df)
        else:
            data.set_named_output(self.named_output, df)

    def assert_is_dataframe(self, df, context):
        ...

    def apply(self, data):
        assert isinstance(data, RuleData)

    def to_dict(self):
        return {
            self.__class__.__name__: {
                attr: value for attr, value in self.__dict__.items() if not attr.startswith("_")
            }
        }

    @classmethod
    def from_dict(cls, dct, backend):
        assert backend and isinstance(backend, str)
        keys = tuple(dct.keys())
        assert len(keys) == 1
        rule_name = keys[0]
        backend_pkg = f'finrules.backends.{backend}'
        mod = importlib.import_module(backend_pkg, '')
        clss = getattr(mod, rule_name, None)
        assert clss, f"Cannot find class {rule_name} in package {backend_pkg}"
        return clss(**dct[rule_name])

    def to_yaml(self):
        return yaml.safe_dump(self.to_dict())

    @classmethod
    def from_yaml(cls, yml, backend):
        dct = yaml.safe_load(yml)
        return cls.from_dict(dct, backend)
