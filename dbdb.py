import storage
import binary_tree


class DBDB:
    def __init__(self, f):
        self._storage = storage.Storage(f)
        self._tree = binary_tree.BinaryTree(self._storage)
        self._closed = False

    def __getitem__(self, key):
        self._assert_not_closed()
        return self._tree.get(key)

    def __setitem__(self, key, value):
        self._assert_not_closed()
        self._tree.set(key, value)

    def __delitem__(self, key):
        self._assert_not_closed()
        self._tree.delete(key)

    def __contains__(self, key):
        self._assert_not_closed()
        return self._tree.get(key) is not None

    def commit(self):
        self._assert_not_closed()
        self._tree.commit()

    def close(self):
        if not self._closed:
            self.commit()
            self._storage.close()
            self._closed = True

    def _assert_not_closed(self):
        if self._closed:
            raise ValueError("Database is closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
