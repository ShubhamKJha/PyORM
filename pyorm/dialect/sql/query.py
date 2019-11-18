__author__ = "ShubhamKJha<skjha832@gmail.com>"

from .types import Column


class Query(object):
    def __new__(cls, result_set):
        obj = object.__new__(cls)
        obj.table = None
        obj.table_map = None
        obj.result = result_set
        obj.raw_result = result_set
        return obj

    def all(self):
        return self.result

    def head(self, rows=5):
        if not self.result:
            return None
        return self.result[:rows]

    def first(self):
        return self.head(1)

    def tail(self, rows=5):
        return self.head(-rows)

    def count(self):
        return len(self.result)

    def filter(self, *args):
        filter_set = []
        for item in self.result:
            flag = True
            for rule in args:
                if not self._filter(item, rule):
                    flag = False
            if flag:
                filter_set.append(item)
        return self._pack_query(filter_set)

    @staticmethod
    def _filter(result, rule):
        value = result.get(rule[0])
        other = rule[2]
        return eval("%s%s%s" % (value, rule[1], other))

    def _pack_query(self, result_set):
        q = Query(self._unpack_query(result_set))
        q.set_table(self.table, self.table_map)
        return q

    def order_by(self, field, desc=False):
        if self.table_map is not None:
            if not isinstance(field, Column):
                raise ValueError('Only Column can be ordered.')
            order_set = self.result.copy()
            order_set.sort(key=lambda x: x.get(field.field_name))
            if desc:
                order_set.reverse()
            return self._pack_query(order_set)
        else:
            raise ValueError("This can't be ordered.")

    def set_table(self, table, table_map):
        self.table = table
        self.table_map = table_map
        self.result = self._build_result_set()

    def _build_result_set(self):
        return [self.table(**dict(zip(self.table_map, item))) for item in self.raw_result]

    @staticmethod
    def _unpack_query(query_list):
        return [item.values() for item in query_list]