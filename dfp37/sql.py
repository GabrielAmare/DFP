from typing import Dict, List, Union


class SQL:
    CREATE_TABLE = "CREATE TABLE"
    WHERE = "WHERE"
    SELECT = "SELECT"
    FROM = "FROM"
    UNIQUE = "UNIQUE"
    NULL = "NULL"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    TEXT = "TEXT"
    TRUE = "TRUE"
    FALSE = "FALSE"
    NOT_NULL = "NOT NULL"
    AUTOINCREMENT = "AUTOINCREMENT"
    PRIMARY_KEY = "PRIMARY KEY"
    DEFAULT = "DEFAULT"
    AND = "AND"
    INSERT_INTO = "INSERT INTO"
    VALUES = "VALUES"


def type_to_query(t: type) -> str:
    if t is bool:
        return SQL.BOOLEAN
    elif t is int:
        return SQL.INTEGER
    elif t is float:
        return SQL.DECIMAL
    elif t is str:
        return SQL.TEXT
    else:
        raise TypeError(t)


def value_to_query(o: object) -> str:
    if o is None:
        return SQL.NULL

    t = type(o)
    if t is bool:
        return {True: SQL.TRUE, False: SQL.FALSE}[o]
    elif t is int:
        return repr(o)
    elif t is float:
        return repr(o)
    elif t is str:
        return repr(o)
    else:
        raise TypeError(t)


def args_to_query(args: List[str], non_terminal: bool = True) -> str:
    return " ".join(args) + ("" if non_terminal else ";")


def _CREATE_COLUMN(column):
    args = [
        column.name,
        type_to_query(column.type)
    ]

    if not column.optional:
        args.append(SQL.NOT_NULL)

    if column.unique:
        args.append(SQL.UNIQUE)

    if column.default is not None:
        args.append(SQL.DEFAULT)
        args.append(value_to_query(column.default))

    if column.primary_key:
        args.append(SQL.PRIMARY_KEY)

    if column.autoincrement:
        args.append(SQL.AUTOINCREMENT)

    return args_to_query(args, non_terminal=True)


def CREATE_TABLE(table_name: str, columns: list):
    return args_to_query([
        SQL.CREATE_TABLE,
        table_name,
        "(",
        ", ".join(map(_CREATE_COLUMN, columns)),
        ")"
    ])


def SELECT_FROM(table_name: str):
    return args_to_query([
        SQL.SELECT,
        "*",
        SQL.FROM,
        table_name,
    ])


def SELECT_FROM_WHERE(table_name: str, config: Dict[str, Union[bool, float, int, str, None]], columns: Dict[str, type]):
    return args_to_query([
        SQL.SELECT,
        "*",
        SQL.FROM,
        table_name,
        SQL.WHERE,
        (" " + SQL.AND + " ").join(
            key + "=" + value_to_query(val)
            for key, val in config.items()
            if key in columns
        )
    ])


def INSERT_INTO(table_name: str, keys: List[str], vals: List[Union[bool, float, int, str, None]]):
    return args_to_query([
        SQL.INSERT_INTO,
        table_name,
        "(",
        ", ".join(keys),
        ")",
        SQL.VALUES,
        "(",
        ", ".join(map(value_to_query, vals)),
        ")"
    ])
