class LogicalBase:
    def __init__(self, storage):
        self._storage = storage
        self._tree_ref = None

    def get(self, key):
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._get(self._follow(self._tree_ref), key)

    def set(self, key, value):
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._insert(self._follow(self._tree_ref), key, value)

    def commit(self):
        self._storage.set_root_address(self._tree_ref.address)

    def _refresh_tree_ref(self):
        self._tree_ref = self.node_ref_class(self._storage.get_root_address())

    def _follow(self, ref):
        return ref.get()
