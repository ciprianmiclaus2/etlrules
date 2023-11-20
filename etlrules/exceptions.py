

class MissingColumnError(Exception):
    """ An operation is being applied to a column that is not present in the input data frame. """


class UnsupportedTypeError(Exception):
    """ A type conversion is attempted to a type that is not supported. """


class ColumnAlreadyExistsError(Exception):
    """ An attempt to create a column that already exists in the dataframe. """


class ExpressionSyntaxError(SyntaxError):
    """ A Python expression used to create a column, aggregate or other operations has a syntax error. """


class SchemaError(Exception):
    """ An operation needs a certain schema for the dataframe which is not present. """


class InvalidPlanError(Exception):
    """ The plan failed validation. """


class GraphRuntimeError(RuntimeError):
    """ There was an error when running a graph-mode plan. """


class SQLError(RuntimeError):
    """ There was an error during the execution of a sql statement. """
