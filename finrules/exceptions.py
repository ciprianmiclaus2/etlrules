

class MissingColumnError(Exception):
    """ An operation is being applied to a column that is not present in the input data frame. """


class UnsupportedTypeError(Exception):
    """ A type conversion is attempted to a type that is not supported. """


class ColumnAlreadyExistsError(Exception):
    """ Raised when trying to create a column that already exists in the dataframe. """
