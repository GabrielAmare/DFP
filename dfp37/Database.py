import sqlite3
import os
from datetime import datetime

from .sql import *

TABLE_ATTRIBUTES = ("database", "allow_overwrite_column", "columns")


class _Column:
    name: str
    type: type
    optional: bool


class _Table:
    allow_overwrite_column: bool = False
    columns: List[_Column]

    def set_column(self, column):
        raise NotImplementedError

    def get_column(self, name):
        raise NotImplementedError


class _Database:
    allow_overwrite_table: bool
    tables: Dict[str, _Table]

    def set_table(self, table):
        raise NotImplementedError

    def get_table(self, name):
        raise NotImplementedError


class Database(_Database):
    def __init__(self, name: str, allow_overwrite_table: bool = False, root: str = os.curdir, query_log_fp=None):
        self.name = name
        self.allow_overwrite_table = allow_overwrite_table
        self.tables = {}

        self.logfp = (query_log_fp or name) + ".log"

        self.path = os.path.abspath(os.path.join(root, name + ".db")).replace("\\", "/")

        self.existed_before_runtime = os.path.exists(self.path)
        self.session_start = datetime.now()

        self.log(f"\nLOG : new session starting at {self.session_start.isoformat()}\n")

        self.con = sqlite3.connect(self.path)
        self.cur = self.con.cursor()

    def __del__(self):
        self.con.commit()
        self.con.close()

    def build(self):
        """Initial build of the database"""

        for table_name, table in self.tables.items():
            query = CREATE_TABLE(table_name, table.columns)
            self.execute(query)

        self.commit()

    def commit(self):
        self.con.commit()

    def log(self, line: str):
        if self.logfp:
            exists = os.path.exists(self.logfp)
            with open(self.logfp, mode="a", encoding="utf-8") as log:
                if not exists:
                    log.write(f"database log for : {self.path}")
                log.write(f"\n{line}")

    def execute(self, query: str):
        if self.logfp:
            exists = os.path.exists(self.logfp)
            self.log(f"SQL : {query}")

        return self.cur.execute(query)

    def set_table(self, table: _Table) -> None:
        if not self.allow_overwrite_table:
            if table.__name__ in self.tables:
                raise ValueError(table, f"can't overwrite {table.__name__}")

        self.tables[table.__name__] = table

    def get_table(self, name: str) -> Union[type, None]:
        return self.tables.get(name)

    def has_table(self, name: str) -> bool:
        return name in self.tables


class Table(_Table):
    columns = []

    def __init_subclass__(cls, allow_overwrite_column: bool = False, **kwargs):
        cls.database = kwargs.pop('database')
        cls.allow_overwrite_column = allow_overwrite_column
        cls.database.set_table(cls)

        cls.columns = []

        for mro in cls.__mro__:
            if issubclass(mro, Table) and mro is not Table:
                for column in cls.columns:
                    cls.set_column(column)

    @classmethod
    def set_column(cls, column: _Column) -> None:
        if not cls.allow_overwrite_column:
            if column.name in [column.name for column in cls.columns]:
                raise ValueError(column, f"can't overwrite {column.name}")

        if cls.has_column(column.name):
            index = cls.get_column_index(column.name)
            cls.columns[index] = column
        else:
            cls.columns.append(column)

    @classmethod
    def get_column(cls, name: str) -> Union[_Column, None]:
        for column in cls.columns:
            if column.name == name:
                return column

    @classmethod
    def get_column_index(cls, name: str):
        for index, column in enumerate(cls.columns):
            if column.name == name:
                return index

    @classmethod
    def has_column(cls, name: str) -> bool:
        return name in (column.name for column in cls.columns)

    @classmethod
    def findall(cls, **config):
        if config:
            query = SELECT_FROM_WHERE(cls.__name__, config, dict((column.name, column.type) for column in cls.columns))
        else:
            query = SELECT_FROM(cls.__name__)

        keys = tuple(column.name for column in cls.columns)

        for vals in cls.database.execute(query):
            data = dict((key, val) for key, val in zip(keys, vals))
            obj = cls(_loaded_from_db=True, **data)
            yield obj

    def __init__(self, _loaded_from_db=False, **config):
        self._ = {}
        for column in self.__class__.columns:
            self._[column.name] = config.pop(column.name, None)

        if config:
            raise ValueError(config, "remaining data in row")

        if not _loaded_from_db:
            query = INSERT_INTO(
                table_name=self.__class__.__name__,
                keys=[column.name for column in self.__class__.columns],
                vals=[self._[column.name] for column in self.__class__.columns]
            )
            self.database.execute(query)

    def __repr__(self):
        return self.__class__.__name__ + \
               "(" + \
               ", ".join(
                   f"{column.name}={repr(self._[column.name])}"
                   for column in self.__class__.columns
               ) + \
               ")"

    def __getattr__(self, key: str):
        if key.startswith("__") and key.endswith("__"):
            return super().__getattribute__(key)
        elif key in TABLE_ATTRIBUTES:
            return super().__getattribute__(key)
        elif self.has_column(key):
            return self._.__getitem__(key)
        else:
            return super().__getattribute__(key)

    def __setattr__(self, key: str, val: object):
        if key.startswith("__") and key.endswith("__"):
            super().__setattr__(key, val)
        elif key in TABLE_ATTRIBUTES:
            super().__setattr__(key, val)
        elif self.has_column(key):
            self._.__setitem__(key, val)
        else:
            super().__setattr__(key, val)


class Column(_Column):
    def __init__(self, name: str, type_: type, optional: bool = False, unique: bool = False,
                 default: Union[bool, int, float, str, None] = None, autoincrement: bool = False,
                 primary_key: bool = False):
        if type_ not in (bool, int, float, str):
            raise TypeError(type_, "this type is not handled by the current version")

        if default is not None and not isinstance(default, type_):
            raise ValueError(default, "default value type doesn't correspond to column type")

        self.name = name
        self.type = type_
        self.optional = optional
        self.unique = unique
        self.default = default
        self.primary_key = primary_key
        self.autoincrement = autoincrement

    def __call__(self, table: _Table):
        table.set_column(self)
        return table
