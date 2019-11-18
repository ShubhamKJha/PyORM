from datetime import datetime


class _Column(object):
    def __init__(self):
        self.field_name = None
        self.python_type = None

    def __lt__(self, other):
        self._check_type(self, other)
        return self.field_name, "<", other

    def __le__(self, other):
        self._check_type(self, other)
        return self.field_name, "<=", other

    def __ge__(self, other):
        self._check_type(self, other)
        return self.field_name, ">=", other

    def __gt__(self, other):
        self._check_type(self, other)
        return self.field_name, ">", other

    def __eq__(self, other):
        self._check_type(self, other)
        return self.field_name, "=", other

    @staticmethod
    def _check_type(field, other):
        if not isinstance(other, field.python_type):
            raise TypeError("Cannot make comparision between %s and %s" % (field.python_type.__name__, other.__class__.__name__))


class Column(_Column):
    __name__ = "Column"

    def __init__(self, sqltype, nullable=True, default=None, auto_increment=False, primary_key=False):
        super(Column, self).__init__()
        self.nullable = nullable
        self.default = default
        self.auto_increment = auto_increment
        # If auto_increment is True so the primary key should be True
        self.primary_key = True if auto_increment else primary_key
        if isinstance(sqltype, BaseType):
            self.sqltype = sqltype
        else:
            raise ValueError("Expected SQLType[Integer|String|Time|Blob], got %s" % sqltype.__class__.__name__)
        self._type = sqltype.type
        self._raw_data = None
        self.table_name = None

    @property
    def python_type(self):
        return self._type

    @python_type.setter
    def python_type(self, value):
        self._type = value

    @property
    def raw(self):
        return self._raw_data

    def fill(self, value):
        self._raw_data = self.python_type(value)


class BaseType(object):
    """
    Basic Type
    """
    def __new__(cls):
        obj = object.__new__(cls)
        obj.type = None
        return obj


# Basic Data Types used in SQL-based databases
class Integer(BaseType):
    __name__ = 'INTEGER'

    def __new__(cls, length=0, real=False):
        obj = super(Integer, cls).__new__(cls)
        obj.type = int
        obj.length = length
        if length in range(1, 256):
            obj.__name__ = 'TINYINT'
        if real:
            obj.type = float
            obj.__name__ = 'FLOAT'
        return obj


class Blob(BaseType):
    __name__ = 'BLOB'

    def __new__(cls):
        obj = super(Blob, cls).__new__(cls)
        obj.type = bytes
        return obj


class Varchar(BaseType):
    __name__ = 'VARCHAR'

    def __new__(cls, length=255):
        obj = super(Varchar, cls).__new__(cls)
        obj.length = length
        obj.type = str
        return obj


class Time(BaseType):
    __name__ = 'TIME'

    def __new__(cls):
        obj = super(Time, cls).__new__(cls)
        obj.type = datetime
        return obj