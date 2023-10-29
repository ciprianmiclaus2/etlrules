import importlib
import yaml
from typing import Optional

from .data import RuleData


class BaseRule:

    EXCLUDE_FROM_COMPARE = ()
    EXCLUDE_FROM_SERIALIZE = ()

    def __init__(self, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        assert named_output is None or isinstance(named_output, str) and named_output
        self.named_output = named_output
        self.name = name
        self.description = description
        self.strict = strict

    def get_name(self) -> Optional[str]:
        return self.name

    def get_description(self) -> Optional[str]:
        return self.description

    def has_input(self) -> bool:
        return True

    def has_output(self) -> bool:
        return True

    def has_named_output(self) -> bool:
        return bool(self.named_output)

    def get_all_named_inputs(self):
        yield from ()

    def get_all_named_outputs(self):
        yield self.named_output

    def _set_output_df(self, data, df):
        if self.named_output is None:
            data.set_main_output(df)
        else:
            data.set_named_output(self.named_output, df)

    def apply(self, data: RuleData) -> None:
        assert isinstance(data, RuleData)

    def to_dict(self) -> dict:
        dct = {
            "name": self.name,
            "description": self.description,
        }
        dct.update({
            attr: value for attr, value in self.__dict__.items() 
                if not attr.startswith("_") and attr not in self.EXCLUDE_FROM_SERIALIZE
                and attr not in dct.keys()
        })
        return {
            self.__class__.__name__: dct
        }

    @classmethod
    def from_dict(cls, dct: dict, backend: str) -> 'BaseRule':
        assert backend and isinstance(backend, str)
        keys = tuple(dct.keys())
        assert len(keys) == 1
        rule_name = keys[0]
        backend_pkg = f'etlrules.backends.{backend}'
        mod = importlib.import_module(backend_pkg, '')
        clss = getattr(mod, rule_name, None)
        assert clss, f"Cannot find class {rule_name} in package {backend_pkg}"
        if clss is not cls:
            return clss.from_dict(dct, backend)
        return clss(**dct[rule_name])

    def to_yaml(self):
        return yaml.safe_dump(self.to_dict())

    @classmethod
    def from_yaml(cls, yml: str, backend: str) -> 'BaseRule':
        dct = yaml.safe_load(yml)
        return cls.from_dict(dct, backend)

    def __eq__(self, other) -> bool:
        return (
            type(self) == type(other) and 
            {k: v for k, v in self.__dict__.items() if k not in self.EXCLUDE_FROM_COMPARE} == 
            {k: v for k, v in other.__dict__.items() if k not in self.EXCLUDE_FROM_COMPARE}
        )


class UnaryOpBaseRule(BaseRule):
    """ Base class for unary operation rules (ie operations taking a single data frame as input). """

    def __init__(self, named_input: Optional[str]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_output=named_output, name=name, description=description, strict=strict)
        assert named_input is None or isinstance(named_input, str) and named_input
        self.named_input = named_input

    def _get_input_df(self, data: RuleData):
        if self.named_input is None:
            return data.get_main_output()
        return data.get_named_output(self.named_input)

    def get_all_named_inputs(self):
        yield self.named_input


class BinaryOpBaseRule(BaseRule):
    """ Base class for binary operation rules (ie operations taking two data frames as input). """

    def __init__(self, named_input_left: Optional[str], named_input_right: Optional[str], named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_output=named_output, name=name, description=description, strict=strict)
        assert named_input_left is None or isinstance(named_input_left, str) and named_input_left
        assert named_input_right is None or isinstance(named_input_right, str) and named_input_right
        self.named_input_left = named_input_left
        self.named_input_right = named_input_right

    def _get_input_df_left(self, data: RuleData):
        if self.named_input_left is None:
            return data.get_main_output()
        return data.get_named_output(self.named_input_left)

    def _get_input_df_right(self, data: RuleData):
        if self.named_input_right is None:
            return data.get_main_output()
        return data.get_named_output(self.named_input_right)

    def get_all_named_inputs(self):
        yield self.named_input_left
        yield self.named_input_right
