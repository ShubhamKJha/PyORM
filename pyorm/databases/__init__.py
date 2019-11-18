__author__ = "ShubhamKJha<skjha832@gmail.com>"

from .base import DatabaseException
from ..pyorm import Engine


class connecting(object):   # context manager for connect_db
    def __init__(self, db_code, db_path, *args, **kwargs):
        self.res = connect_db(db_code, db_path, *args, **kwargs)

    def __enter__(self):
        return self.res

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.res.disconnect()


def connect_db(db_code, db_path, *args, **kwargs):
    """
    This function connects to different databases that can be used within PYORM.
    Currently, two of these are supported

        1) db_code :: sqlite
            The appropriate syntax should be:
            connect_db( 'sqlite',
                        'path/to/database',
                        timeout = timeout,
                        isolation_level = isolation_level,
                        uri = uri,
                        **kwargs)
        2) db_code :: mysql
            The appropriate syntax should be:
            connect_db( 'mysql',
                        'path/to/database',
                         username = username,
                         password = password,
                         host = host,
                         port = port,
                         timeout = timeout,
                         **kwargs)
    Any other db_code would lead to Execption.

    :param db_code: Currently supports 'sqlite' and 'mysql'
    :param db_path: Path to the respective database
    :param args:
    :param kwargs:
    :return: Returns an engine used for interacting with databases
    """
    # TODO: Add other databases
    if db_code == 'sqlite':
        try:
            import sqlite3
            from .sqlite import SqliteDatabase
            return Engine(SqliteDatabase(db_path, *args, **kwargs))
        except ImportError:
            raise DatabaseException("'sqlite3' module is not present which is a requisite.")
    elif db_code == 'mysql':
        try:
            import pymysql
            from .mysql import MysqlDatabase
            return Engine(MysqlDatabase(db_path, *args, **kwargs))
        except ImportError:
            raise DatabaseException("'pymysql' module is not present which is a requisite.")
    else:
        raise NotImplementedError('Database Connection for %s is still to be implemented.' % db_code)