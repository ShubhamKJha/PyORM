__author__ = "ShubhamKJha<skjha832@gmail.com>"

import pymysql
from databases.base import BaseDatabase, DatabaseException


class MysqlDatabase(BaseDatabase):
    def __new__(cls, db, *args, **kwargs):
        obj = BaseDatabase.__new__(cls, db, *args, **kwargs)
        obj.name = 'mysql'
        obj.connect()
        obj.cursor = obj.engine.cursor()
        return obj

    def connect(self):
        if self.connected:
            return self.engine

        try:
            self.kwargs['database'] = self.db
            self.engine = pymysql.connect(**self.kwargs)
            self._connected = True
            return self.engine
        except pymysql.OperationalError as e:
            raise DatabaseException(e)

    def close(self):
        if self.connected:
            self.cursor.close()
            self.engine.close()
        self._connected = False

    def commit(self):
        self.engine.commit()

    def execute(self, query, *args, **kwargs):
        self.cursor.execute(query, *args)
