

class MissingColumnError(Exception):
    """ An operation is being applied to a column that is not present in the input data frame. """


class UnsupportedTypeError(Exception):
    """ A type conversion is attempted to a type that is not supported. """


class ColumnAlreadyExistsError(Exception):
    """ An attempt to create a column that already exists in the dataframe. """


class AddNewColumnSyntaxError(SyntaxError):
    """ A column is created but there is a syntax error in the column expression. """