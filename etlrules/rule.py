import importlib
import yaml
from typing import Generator, Optional, Sequence

from etlrules.data import RuleData
from etlrules.exceptions import ColumnAlreadyExistsError, MissingColumnError


class BaseRule:
    """ The base class for all rules.

    Derive your custom rules from BaseRule in order to use them in a plan.
    Implement the following methods as needed:
    apply: mandatory, it implements the functionality of the rule
    has_input: defaults to True, override and return False if your rule reads data
        into the plan and therefore has no other dataframe input
    has_output: defaults to True, override and return False if your rule writes data
        to a persistent repository and therefore has no dataframe output
    get_all_named_inputs: override to return the named inputs (if any) as strings
    get_all_named_outputs: override in case of multiple named outputs and return them as strings

    named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
        When not set, the result of this rule will be available as the main output.
        When set to a name (string), the result will be available as that named output.
    name (Optional[str]): Give the rule a name. Optional.
        Named rules are more descriptive as to what they're trying to do/the intent.
    description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
        Together with the name, the description acts as the documentation of the rule.
    strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Note:
        Add any class data members to the following list/tuples if needed:
        EXCLUDE_FROM_COMPARE: Used in implementing equality between rules. Equality is
            mostly used in tests. By default, equality looks at all data members in the
            class' __dict__. You can exclude calculated or transient data members which
            should be excluded from equality. Alternatively, you can implement __eq__ in
            your own class and not rely on the __eq__ implementation in the base class.
        EXCLUDE_FROM_SERIALIZE: Used to exclude data members from the serialization to
            dict and yaml. The serialization is implemented generically in the base class
            to serialize all data members in the class' __dict__ which do not start with
            an underscore. See the note on serialization below.
    
    Note:
        When implementing serialization, the arguments into your class should be saved as
        they are in data members with the same name as the arguments. This is because the
        de-serialization passes those as args into the __init__. As such, make sure to use
        the same names and to exclude data members which are not in the __init__ from
        serialization by adding them to EXCLUDE_FROM_SERIALIZE.

    """

    EXCLUDE_FROM_COMPARE = ()
    EXCLUDE_FROM_SERIALIZE = ()

    def __init__(self, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        assert named_output is None or isinstance(named_output, str) and named_output
        self.named_output = named_output
        self.name = name
        self.description = description
        self.strict = strict

    def get_name(self) -> Optional[str]:
        """ Returns the name of the rule.

        The name is optional and it can be None.

        The name of the rule should indicate what the rule does and not how it's
        implemented. The names should read like documentation. As such, names like

        Remove duplicate first names from the list of addresses
        Only keep the names starting with A

        are preferable names to:

        DedupeRule
        ProjectRule

        Names are not used internally for anything other than your own (and your 
        end users') documentation, so use what makes sense.
        """
        return self.name

    def get_description(self) -> Optional[str]:
        """ A long description of what the rule does, why and optionally how it does it.

        The description is optional and it can be None.

        Similar to name, this long description acts as documentation for you and your users.
        It's particularly useful if your rule is serialized in a readable format like yaml
        and your users either do not have access to the documentation or they are not technical.

        Unlike the name, which should generally be a single line headline, the description is a
        long, multi-line description of the rule: the what, why, how of the rule.
        """
        return self.description

    def has_input(self) -> bool:
        """ Returns True if the rule needs a dataframe input to operate on, False otherwise.

        By default, it returns True. It should be overriden to return False for those
        rules which read data into the plan. For example, reading a csv file or reading a
        table from the DB. These are operation which do not need an input dataframe to
        operate on as they are sourcing data.
        """
        return True

    def has_output(self) -> bool:
        """ Returns True if the rule produces a dataframe, False otherwise.

        By default, it returns True. It should be overriden to return False for those
        rules which write data out of the plan. For example, writing a file or data into a
        database. These are operations which do not produce an output dataframe into
        the plan as they are writing data outside the plan.
        """
        return True

    def has_named_output(self) -> bool:
        return bool(self.named_output)

    def get_all_named_inputs(self) -> Generator[str, None, None]:
        """ Yields all the named inputs of this rule (as strings).

        By default, it yields nothing as this base rule doesn't store
        information about inputs. Some rules take no input, some take
        one or more inputs. Yield accordingly when you override.
        """
        yield from ()

    def get_all_named_outputs(self) -> Generator[str, None, None]:
        """ Yields all the named outputs of this rule (as strings).

        By default, it yields the single named_output passed into this
        rule as an argument. Some rules produce no output, some produce
        multiple outputs. Yield accordingly when you override.
        """
        yield self.named_output

    def _set_output_df(self, data, df):
        if self.named_output is None:
            data.set_main_output(df)
        else:
            data.set_named_output(self.named_output, df)

    def apply(self, data: RuleData) -> None:
        """ Applies the main rule logic to the input data.

        This is the main rule that applies a rule logic to an input data.
        The input data is an instance of RuleData which can store a single, unnamed
        dataframe (in pipeline mode) or one or many named dataframes (in graph mode).
        The rule extracts the data it needs from the data, applies its main logic
        and updates the same instance of RuleData with the output, if any.

        This method doesn't do anything in the base class other than asserting that
        the data passed in is an instance of RuleData. Override this when you derive
        from BaseRule and implement the logic of your rule.

        """
        assert isinstance(data, RuleData)

    def to_dict(self) -> dict:
        """ Serializes this rule to a python dictionary.

        This is a generic implementation that should work for all derived
        classes and therefore you shouldn't need to override, although you can do so.

        Because it aims to be generic and work correctly for all the derived classes,
        a few assumptions are made and must be respected when you implement your own
        rules derived from BaseRule.

        The class will serialize all the data attributes of a class which do not start with
        underscore and are not explicitly listed in the EXCLUDE_FROM_SERIALIZE static member
        of the class. As such, to exclude any of your internal data attributes, either named
        them so they start with an underscore or add them explicitly to EXCLUDE_FROM_SERIALIZE.

        The serialize will look into a classes __dict__ and therefore the class must have a
        __dict__.

        For the de-serialization to work generically, the name of the attributes must match the
        names of the arguments in the __init__. This is quite an important and restrictive
        constraint which is needed to avoid forcing every rule to implement a serialize/deserialize.
        
        Note:
            Use the same name for attributes on self as the respective arguments in __init__.

        """
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
    def from_dict(cls, dct: dict, backend: str, additional_packages: Optional[Sequence[str]]=None) -> 'BaseRule':
        """ Creates a rule instance from a python dictionary.

        Args:
            dct: A dictionary to create the plan from
            backend: One of the supported backends (ie pandas)
            additional_packages: Optional list of other packages to look for rules in
        Returns:
            A new instance of a Plan.
        """
        assert backend and isinstance(backend, str)
        keys = tuple(dct.keys())
        assert len(keys) == 1
        rule_name = keys[0]
        backend_pkgs = [f'etlrules.backends.{backend}']
        for additional_package in additional_packages or ():
            backend_pkgs.append(additional_package)
        modules = [importlib.import_module(backend_pkg, '') for backend_pkg in backend_pkgs]
        for mod in modules:
            clss = getattr(mod, rule_name, None)
            if clss is not None:
                break
        assert clss, f"Cannot find class {rule_name} in packages: {backend_pkgs}"
        if clss is not cls:
            return clss.from_dict(dct, backend, additional_packages)
        return clss(**dct[rule_name])

    def to_yaml(self):
        """ Serialize the rule to yaml. """
        return yaml.safe_dump(self.to_dict())

    @classmethod
    def from_yaml(cls, yml: str, backend: str, additional_packages: Optional[Sequence[str]]=None) -> 'BaseRule':
        """ Creates a rule instance from a yaml definition.

        Args:
            yml: The yaml string to create the plan from
            backend: A supported backend (ie pandas)
            additional_packages: Optional list of other packages to look for rules in
        
        Returns:
            A new instance of a rule.
        """
        dct = yaml.safe_load(yml)
        return cls.from_dict(dct, backend, additional_packages)

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


class ColumnsInOutMixin:
    def validate_input_column(self, df_columns: Sequence[str], input_column: str, strict: bool):
        if input_column not in df_columns:
            raise MissingColumnError(f"Column '{input_column}' is missing from the input dataframe.")
        return input_column

    def validate_output_column(self, df_columns: Sequence[str], input_column: str, output_column: Optional[str], strict: bool):
        if output_column is not None:
            if strict and output_column in df_columns:
                raise ColumnAlreadyExistsError(f"Column '{output_column}' already exists in the input dataframe.")
            return output_column
        return input_column

    def validate_in_out_columns(self, df_columns: Sequence[str], input_column: str, output_column: Optional[str], strict: bool):
        input_column = self.validate_input_column(df_columns, input_column, strict)
        output_column = self.validate_output_column(df_columns, input_column, output_column, strict)
        return input_column, output_column

    def validate_columns_in(self, df_columns: Sequence[str], columns: Sequence[str], strict: bool) -> Sequence[str]:
        if not set(columns) <= set(df_columns):
            raise MissingColumnError(f"Column(s) {set(columns) - set(df_columns)} are missing from the input dataframe.")
        return columns

    def validate_columns_out(self, df_columns: Sequence[str], columns: Sequence[str], output_columns: Optional[Sequence[str]], strict: bool, validate_length: bool=True) -> Sequence[str]:
        if output_columns:
            if strict:
                existing_columns = set(output_columns) & set(df_columns)
                if existing_columns:
                    raise ColumnAlreadyExistsError(f"Column(s) already exist: {existing_columns}")
            if validate_length and len(output_columns) != len(columns):
                raise ValueError(f"output_columns must be of the same length as the columns: {columns}")
        else:
            output_columns = columns
        return output_columns

    def validate_columns_in_out(self, df_columns: Sequence[str], columns: Sequence[str], output_columns: Optional[Sequence[str]], strict: bool, validate_length: bool=True) -> tuple[Sequence[str], Sequence[str]]:
        columns = self.validate_columns_in(df_columns, columns, strict)
        output_columns = self.validate_columns_out(df_columns, columns, output_columns, strict, validate_length=validate_length)
        return columns, output_columns


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
