

class MissingColumn(Exception):
    """ An operation is being applied to a column that is not present in the input data frame. """


class UnsupportedType(Exception):
    """ A type conversion is attempted to a type that is not supported. """
