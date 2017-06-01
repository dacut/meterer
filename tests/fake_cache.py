"""
A fake cache that emulates just enough of the Redis interface to make unit
testing work.
"""
# pylint: disable=C0111,E1129
from threading import RLock
from time import time


class FakeCache(object):
    """
    A fake cache that emulates just enough of the Redis interface to make unit
    testing work.
    """

    def __init__(self):
        super(FakeCache, self).__init__()
        self.data = {}
        self.expires = {}
        self.lock = RLock()
        return

    def get(self, key):
        with self.lock:
            expires = self.expires.get(key)
            if expires is not None and time() > expires:
                del self.expires[key]
                del self.data[key]
                return None

            return self.data.get(key)

    def set(self, key, value, ex=None):
        with self.lock:
            self.data[key] = str(value)
            if ex is None:
                if key in self.expires:
                    del self.expires[key]
            else:
                self.expires[key] = time() + ex
            return

    def incrbyfloat(self, key, amount=1.0):
        with self.lock:
            value = self.get(key)
            if value is None:
                value = amount
            else:
                value = float(value) + amount

            self.data[key] = str(value)

            return value
