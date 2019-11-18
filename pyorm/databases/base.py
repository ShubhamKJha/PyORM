__author__ = "ShubhamKJha<skjha832@gmail.com>"


class BaseDatabase(object):
    """
    Abstract class for all databases
    """

    def __new__(cls, db, *args, **kwargs):
        obj = object.__new__(cls)
        obj.db = db  # Database file
        obj.args = args
        obj.kwargs = kwargs
        obj._connected = False
        obj.engine = None
        obj.cursor = None
        obj.name = 'BaseDatabse'
        return obj

    # Basic properties of all databases
    @property
    def connected(self):
        return self._connected

    def connect(self):
        pass

    def close(self):
        pass

    def commit(self):
        pass

    def execute(self, query, *args, **kwargs):
        pass

    # Extra features
    def __str__(self):
        return '%s : %s' % (self.name, self.db)

    __repr__ = __str__


class DatabaseException(BaseException):
    """
    Exception Specific to Database Errors
    """
    pass
