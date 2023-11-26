try:
    import sqlalchemy as sa
    HAS_SQL_ALCHEMY = True
except ImportError:
    HAS_SQL_ALCHEMY = False
from typing import Mapping, Optional

from etlrules.data import context
from etlrules.backends.common.substitution import OSEnvironSubst
from etlrules.backends.common.types import SUPPORTED_TYPES
from etlrules.exceptions import SQLError, UnsupportedTypeError
from etlrules.rule import BaseRule, UnaryOpBaseRule


class SQLAlchemyEngines:
    ENGINES = {}

    @classmethod
    def get_engine(cls, sql_engine: str):
        engine = cls.ENGINES.get(sql_engine)
        if engine is None:
            assert HAS_SQL_ALCHEMY, "Missing sqlalchemy. pip install SQLAlchemy to resolve."
            engine = sa.create_engine(sql_engine)
            cls.ENGINES[sql_engine] = engine
        return engine


class ReadSQLQueryRule(BaseRule):
    """ Runs a SQL query and reads the results back into a dataframe.

    Basic usage::

        # reads all the data from a sqlite db called mydb.db, from the table MyTable
        # saves the dataframe as the main output of the rule which subsequent rules can use as their main input
        rule = ReadSQLQueryRule("sqlite:///mydb.db", "SELECT * FROM MyTable")
        rule.apply(data)

        # reads all the data from a sqlite db called mydb.db, from the table MyTable
        # saves the dataframe as the a named output called MyData which subsequent rules can use by name
        rule = ReadSQLQueryRule("sqlite:///mydb.db", "SELECT * FROM MyTable", named_output="MyData")
        rule.apply(data)

        # same as the first example, but uses column types rather than relying on type inferrence
        rule = ReadSQLQueryRule("sqlite:///mydb.db", "SELECT * FROM MyTable", column_types={"ColA": "int64", "ColB": "string"})
        rule.apply(data)

    Args:
        sql_engine: A sqlalchemy engine string. This is typically in the form:
            dialect+driver://username:password@host:port/database
            For more information, please refer to the sqlalchemy documentation here:
            https://docs.sqlalchemy.org/en/20/core/engines.html

            In order to support users and passwords in the sql_engine string, substitutions of environment variables
            is supported using the {env.VARIABLE_NAME} form.
            For example, adding the USER and PASSWORD environment variables in the sql string could be done as:
                sql_engine = "postgres://{env.USER}:{env.PASSWORD}@{env.DB_HOST}/mydb
            In this example, when you run, env.USER, env.PASSWORD and env.DB_HOST will be replaced with the respective
            environment variables, allowing you to not hardcode them in the plan for security reasons but also for
            configurability.
            A similar substition can be achieved using the plan context using the context.property, e.g.
                sql_engine = "postgres://{context.USER}:{env.PASSWORD}@{context.DB_HOST}/mydb
            It's not recommended to store passwords in plain text in the plan.
        sql_query: A SQL SELECT statement that will specify the columns, table and optionally any WHERE, GROUP BY, ORDER BY clauses.
            The SQL statement must be valid for the SQL engine specified in the sql_engine parameter.

            The env and context substitution work in the sql_query too. E.g.:
                SELECT * from {env.SCHEMA}.{context.TABLE_NAME} WHERE {context.FILTER}
            This allows you to parameterize the plan at run time.
        column_types: A mapping of column names and their types. Column types are inferred from the data when this parameter
            is not specified. For empty result sets, this inferrence is not possible, so specifying the column types allows
            the users to control the types in that scenario and not fallback onto backends defaults. 

        named_output (Optional[str]): Give the output of this rule a name so it can be used by another rule as a named input. Optional.
            When not set, the result of this rule will be available as the main output.
            When set to a name (string), the result will be available as that named output.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True

    Raises:
        SQLError: raised if there's an error running the sql statement.
        UnsupportedTypeError: raised if column_types are specified and any of them are not supported.

    Note:
        The implementation uses sqlalchemy, which must be installed as an optional dependency of etlrules.
    """

    def __init__(self, sql_engine: str, sql_query: str, column_types: Optional[Mapping[str, str]]=None, named_output: Optional[str]=None, name: Optional[str]=None, description: Optional[str]=None, strict: bool=True):
        super().__init__(named_output=named_output, name=name, description=description, strict=strict)
        self.sql_engine = sql_engine
        self.sql_query = sql_query
        if not self.sql_engine or not isinstance(self.sql_engine, str):
            raise ValueError("The sql_engine parameter must be a non-empty string.")
        if not self.sql_query or not isinstance(self.sql_query, str):
            raise ValueError("The sql_query parameter must be a non-empty string.")
        self.column_types = column_types
        self._validate_column_types()

    def _validate_column_types(self):
        if self.column_types is not None:
            for column, column_type in self.column_types.items():
                if column_type not in SUPPORTED_TYPES:
                    raise UnsupportedTypeError(f"Type '{column_type}' for column '{column}' is not supported.")

    def has_input(self):
        return False

    def _do_apply(self, connection):
        raise NotImplementedError("Can't instantiate base class.")

    def _get_sql_engine(self):
        return self.sql_engine.format(env=OSEnvironSubst(), context=context)

    def _get_sql_query(self):
        return self.sql_query.format(env=OSEnvironSubst(), context=context)

    def apply(self, data):
        super().apply(data)
        sql_engine = self._get_sql_engine()
        engine = SQLAlchemyEngines.get_engine(sql_engine)
        with engine.connect() as connection:
            try:
                result = self._do_apply(connection)
            except sa.exc.SQLAlchemyError as exc:
                raise SQLError(str(exc))
        self._set_output_df(data, result)


class WriteSQLTableRule(UnaryOpBaseRule):
    """ Writes the data from the input dataframe into a SQL table in a database.

    The rule is a final rule, which means it produces no additional outputs, it takes any of the existing
    outputs and writes it to the DB. If the named_input is specified, the input with that name
    is written, otherwise, it takes the main output of the preceding rule.

    Basic usage::

        # writes the main input to a table called MyTable in a sqlite DB called mydb.db
        # If the table already exists, it replaces it
        rule = WriteSQLTableRule("sqlite:///mydb.db", "MyTable", if_exists="replace")
        rule.apply(data)

        # writes the dataframe input called 'input_data' to a table MyTable in a sqlite DB mydb.db
        # If the table already exists, it appends the data to it
        rule = WriteSQLTableRule("sqlite:///mydb.db", "MyTable", if_exists="append", named_input="input_data")
        rule.apply(data)

    Args:
        sql_engine: A sqlalchemy engine string. This is typically in the form:
            dialect+driver://username:password@host:port/database
            For more information, please refer to the sqlalchemy documentation here:
            https://docs.sqlalchemy.org/en/20/core/engines.html

            In order to support users and passwords in the sql_engine string, substitutions of environment variables
            is supported using the {env.VARIABLE_NAME} form.
            For example, adding the USER and PASSWORD environment variables in the sql string could be done as:
                sql_engine = "postgres://{env.USER}:{env.PASSWORD}@{env.DB_HOST}/mydb
            In this example, when you run, env.USER, env.PASSWORD and env.DB_HOST will be replaced with the respective
            environment variables, allowing you to not hardcode them in the plan for security reasons but also for
            configurability. 
        sql_table: The name of the sql table to write to.
        if_exists: Specifies what to do in case the table already exists in the database.
            The options are:
                - replace: drops all the existing data and inserts the data in the input dataframe
                - append: adds the data in the input dataframe to the existing data in the table
                - fail: Raises a ValueError exception
            Default: fail.

        named_input (Optional[str]): Select by name the dataframe to write from the input data.
            Optional. When not specified, the main output of the previous rule will be written.
        name (Optional[str]): Give the rule a name. Optional.
            Named rules are more descriptive as to what they're trying to do/the intent.
        description (Optional[str]): Describe in detail what the rules does, how it does it. Optional.
            Together with the name, the description acts as the documentation of the rule.
        strict (bool): When set to True, the rule does a stricter valiation. Default: True.

    Raises:
        ValueError: raised if the table already exists when the if_exists is fail.
            ValueError is also raised if any of the arguments passed into the rule are not strings or empty strings.
        SQLError: raised if there's any problem writing the data into the database.
            For example: If the schema doesn't match the schema of the table written to (for existing tables).
    """

    class IF_EXISTS_OPTIONS:
        APPEND = 'append'
        REPLACE = 'replace'
        FAIL = 'fail'

    ALL_IF_EXISTS_OPTIONS = {IF_EXISTS_OPTIONS.APPEND, IF_EXISTS_OPTIONS.REPLACE, IF_EXISTS_OPTIONS.FAIL}

    EXCLUDE_FROM_SERIALIZE = ("named_output", )

    def __init__(self, sql_engine: str, sql_table: str, if_exists: str='fail', named_input=None, name=None, description=None, strict=True):
        super().__init__(named_input=named_input, named_output=None, name=name, description=description, strict=strict)
        self.sql_engine = sql_engine
        if not self.sql_engine or not isinstance(self.sql_engine, str):
            raise ValueError("The sql_engine parameter must be a non-empty string.")
        self.sql_table = sql_table
        if not self.sql_table or not isinstance(self.sql_table, str):
            raise ValueError("The sql_table parameter must be a non-empty string.")
        self.if_exists = if_exists
        if self.if_exists not in self.ALL_IF_EXISTS_OPTIONS:
            raise ValueError(f"'{if_exists}' is not a valid value for the if_exists parameter. It must be one of: '{self.ALL_IF_EXISTS_OPTIONS}'")

    def has_output(self):
        return False
