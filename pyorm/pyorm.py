__author__ = "ShubhamKJha<skjha832@gmail.com>"

from .dialect.sql.types import Column
from .dialect.sql.query import Query
from .dialect.sql import mapper
from .databases.base import BaseDatabase


class Engine(object):
    def __init__(self, conn):
        if not isinstance(conn, BaseDatabase):
            raise TypeError('The type of conn is not correct.')
        self.connector = conn
        self._logging = False
        mapper.target = conn.name

    def insert(self, obj):
        sql = self._mapping_proxy(mapper.INSERT, obj=obj)
        self.send(sql)

    def delete(self, obj):
        sql = self._mapping_proxy(mapper.DELETE, obj=obj)
        self.send(sql)

    def update(self, obj):
        # TODO: Complete Update part
        pass

    def query(self, item):
        sql = self._mapping_proxy(mapper.SELECT, table=item)
        self.send(sql)
        query = Query(self.connector.cursor.fetchall())
        if item in Model.__subclasses__():
            query.set_table(item, item.map)
        return query

    def _drop(self, table):
        sql = self._mapping_proxy(mapper.DROP, table=table)
        self.send(sql)

    def drop(self, *tables):
        for t in tables:
            self._drop(t)

    def drop_all(self):
        for table in Model.__subclasses__():
            self.drop(table)

    def _create(self, table):
        sql = self._mapping_proxy(mapper.CREATE, table=table)
        self.send(sql)

    def create(self, *args):
        for item in args:
            self._create(item)

    def create_all(self):
        for table in Model.__subclasses__():
            self.create(table)

    def commit(self):
        if self.connector.commit:
            self.connector.commit()

    def rollback(self):
        if not hasattr(self.connector.engine, 'rollback'):
            raise NotImplementedError('Roll back feature needs to be implemented for other databases.')
        if self.connector.engine.rollback:
            self.connector.engine.rollback()

    def disconnect(self):
        self.connector.close()
        self.connector = None

    def close(self):
        self.disconnect()

    def send(self, sql_query):
        if self._logging:
            print(sql_query)
        self.connector.execute(sql_query)

    def __str__(self):
        return "<PyORM target={} database={}>".format(self.connector.name, self.connector.db)

    __repr__ = __str__

    def logging(self, logging=True):
        self._logging = logging

    @staticmethod
    def _mapping_proxy(action, table=None, obj=None):
        if action in [mapper.CREATE, mapper.SELECT]:
            if table in Model.__subclasses__() \
                    or table.__class__ is Column:
                sql = mapper.get_sql(action, table)
            else:
                raise ValueError("Expected the a Table class or a Field instance, got %s" % table.__class__.__name__)
        elif action in [mapper.INSERT, mapper.DELETE]:
            table = obj.__class__
            sql = mapper.get_sql(action, table, obj)
        else:
            sql = mapper.get_sql(action, table, obj)
        return sql


class _ModelMetaClass(type):
    """
    MetaClass for the Model class
    """
    def __new__(mcs, name, bases, attrs):
        if name == "Model":
            return type.__new__(mcs, name, bases, attrs)
        if '__table_name__' not in attrs:
            attrs['__table_name__'] = name.lower()

        # For subclasses of Model, we will follow this
        attrs.update({"map": []})
        table_name = attrs['__table_name__']

        for key, attr in attrs.items():
            if isinstance(attr, Column):
                attr.table_name = table_name
                attr.field_name = key
                attrs['map'].append(key)

        return type.__new__(mcs, name, bases, attrs)


class Model(dict, metaclass=_ModelMetaClass):
    """
    As the table inherit from the native dict Class, it's easy to use.
    """
    __table_name__ = ""

    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)
        for k, v in vars(self.__class__).items():
            if isinstance(v, Column):
                if getattr(v, 'default') is not None:
                    self.setdefault(k, getattr(v, 'default'))
                    v.fill(getattr(v, 'default'))
                else:
                    if k not in kwargs and getattr(v, 'auto_increment') is None:
                        raise KeyError('Unspecified key %s with no default attribute' % k)
                if k in kwargs:
                    v.fill(kwargs[k])

    @classmethod
    def get_field(cls):
        fields = []
        for k, v in vars(cls).items():
            if isinstance(v, Column):
                fields.append(v)
        return fields
