class BinaryNode:
    def __init__(self, key, value, left=None, right=None):
        self.key = key
        self.value = value
        self.left = left
        self.right = right


class NodeRef:
    def __init__(self, node=None, address=None):
        self.node = node
        self.address = address  # Will be set by storage when persisted

    def __reduce__(self):
        return (self.__class__, (self.node, self.address))

    def get(self):
        return self.node

    def __str__(self):
        if self.node:
            return f"NodeRef(address={self.address}, key={self.node.key}, value={self.node.value})"
        return f"NodeRef(address={self.address}, node=None)"


class BinaryTree:
    def __init__(self, storage):
        self._storage = storage
        self.root_ref = None
        # Load root if it exists
        try:
            root_address = (
                self._storage.get_root_address()
            )  # Retrieve the root address from storage
            if root_address is not None:
                self.root_ref = self._storage.get(
                    root_address
                )  # Retrieve the root node from storage
        except (AttributeError, KeyError):
            pass  # No root address stored yet, handle gracefully

    def set(self, key, value):
        """Insert or update a key-value pair in the tree."""
        if self.root_ref is None:
            # Tree is empty, create a new root node
            new_node = BinaryNode(key, value)
            self.root_ref = NodeRef(new_node)
            self.root_ref.address = self._storage.set(
                None, self.root_ref
            )  # Store root node
        else:
            # Find the appropriate position in the tree
            self._insert(self.root_ref, key, value)

    def _insert(self, node_ref, key, value):
        """Recursive helper for insertion."""
        node = node_ref.get()
        if key == node.key:
            # Update existing value
            node.value = value
            node_ref.address = self._storage.set(
                node_ref.address, node_ref
            )  # Update address in storage
        elif key < node.key:
            if node.left is None:
                # Create new left child
                new_node = BinaryNode(key, value)
                node.left = NodeRef(new_node)
                node.left.address = self._storage.set(None, node.left)
                node_ref.address = self._storage.set(
                    node_ref.address, node_ref
                )  # Update address in storage
            else:
                self._insert(node.left, key, value)
        else:
            if node.right is None:
                # Create new right child
                new_node = BinaryNode(key, value)
                node.right = NodeRef(new_node)
                node.right.address = self._storage.set(None, node.right)
                node_ref.address = self._storage.set(
                    node_ref.address, node_ref
                )  # Update address in storage
            else:
                self._insert(node.right, key, value)

    def get(self, key):
        """Retrieve a value by key."""
        if self.root_ref is None:
            return None
        return self._search(self.root_ref, key)

    def _search(self, node_ref, key):
        """Recursive helper for search."""
        if node_ref is None or node_ref.get() is None:
            return None

        node = node_ref.get()
        if key == node.key:
            return node.value
        elif key < node.key:
            return self._search(node.left, key)
        else:
            return self._search(node.right, key)

    # def commit(self):
    #     """Persist the entire tree to storage."""
    #     if self.root_ref:
    #         # First commit all nodes recursively
    #         self._commit_node(self.root_ref)
    #         # Then store the root address
    #         if hasattr(self._storage, "commit_root_address"):
    #             self._storage.commit_root_address(self.root_ref.address)
    def commit(self):
        if self.root_ref:
            self._commit_node(self.root_ref)
            self._storage.commit_root_address(self.root_ref.address)
            self._storage.flush()  # ← Add this line

    def _commit_node(self, node_ref):
        """Recursively commit a node and its children."""
        if node_ref and node_ref.get():
            # Commit children first (post-order traversal might be better for some cases)
            if node_ref.get().left:
                self._commit_node(node_ref.get().left)
            if node_ref.get().right:
                self._commit_node(node_ref.get().right)
            # Commit this node to storage
            node_ref.address = self._storage.set(node_ref.address, node_ref)

    def __contains__(self, key):
        """Support for 'in' operator."""
        return self.get(key) is not None
