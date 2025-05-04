import pickle
from .logical import LogicalBase, ValueRef


class BinaryTree(LogicalBase):
    """
    Concrete implementation of a persistent binary tree.
    Uses immutable nodes and copy-on-write semantics.
    """

    # Reference class for tree nodes
    node_ref_class = None  # Will be set to BinaryNodeRef after class definition

    def _get(self, node, key):
        """
        Retrieve a value from the binary tree.

        Args:
            node: Current node in traversal
            key: Key to search for

        Returns:
            The value associated with the key

        Raises:
            KeyError: If key not found
        """
        while node is not None:
            if key < node.key:
                node = self._follow(node.left_ref)
            elif node.key < key:
                node = self._follow(node.right_ref)
            else:
                return self._follow(node.value_ref)
        raise KeyError

    def _insert(self, node, key, value_ref):
        """
        Insert or update a key/value pair in the tree.
        Returns new node rather than modifying existing nodes (immutable).

        Args:
            node: Current node in traversal
            key: Key to insert
            value_ref: Reference to value to associate with key

        Returns:
            New BinaryNodeRef containing updated tree
        """
        if node is None:
            # Create new leaf node
            new_node = BinaryNode(
                self.node_ref_class(),  # left_ref
                key,  # key
                value_ref,  # value_ref
                self.node_ref_class(),  # right_ref
                1,  # length
            )
        elif key < node.key:
            # Recursively update left subtree
            new_node = BinaryNode.from_node(
                node, left_ref=self._insert(self._follow(node.left_ref), key, value_ref)
            )
        elif node.key < key:
            # Recursively update right subtree
            new_node = BinaryNode.from_node(
                node,
                right_ref=self._insert(self._follow(node.right_ref), key, value_ref),
            )
        else:
            # Update existing key
            new_node = BinaryNode.from_node(node, value_ref=value_ref)

        return self.node_ref_class(referent=new_node)

    def _delete(self, node, key):
        """
        Delete a key/value pair from the tree.

        Args:
            node: Current node in traversal
            key: Key to delete

        Returns:
            New BinaryNodeRef with key removed

        Raises:
            KeyError: If key not found
        """
        if node is None:
            raise KeyError
        elif key < node.key:
            new_node = BinaryNode.from_node(
                node, left_ref=self._delete(self._follow(node.left_ref), key)
            )
        elif node.key < key:
            new_node = BinaryNode.from_node(
                node, right_ref=self._delete(self._follow(node.right_ref), key)
            )
        else:
            # Found our node to delete
            if node.left_ref.address is None and node.right_ref.address is None:
                # Leaf node - simple deletion
                return self.node_ref_class()
            elif node.left_ref.address is None:
                # Only right child - promote right subtree
                return node.right_ref
            elif node.right_ref.address is None:
                # Only left child - promote left subtree
                return node.left_ref
            else:
                # Two children - complex case
                left = self._follow(node.left_ref)
                right = self._follow(node.right_ref)
                # Find successor (leftmost node in right subtree)
                successor, parent = self._find_min(right, node.right_ref)
                # Remove successor from its original position
                new_right = self._delete_min(right)
                # Replace deleted node with successor
                new_node = BinaryNode(
                    node.left_ref,
                    successor.key,
                    successor.value_ref,
                    new_right,
                    left.length + new_right.length + 1,
                )

        return self.node_ref_class(referent=new_node)

    def _find_min(self, node, node_ref):
        """
        Find minimum node in subtree (helper for deletion).

        Args:
            node: Current node in traversal
            node_ref: Reference to current node

        Returns:
            Tuple of (min_node, parent_ref)
        """
        while self._follow(node.left_ref) is not None:
            node_ref = node.left_ref
            node = self._follow(node.left_ref)
        return node, node_ref

    def _delete_min(self, node):
        """
        Delete minimum node in subtree (helper for deletion).

        Args:
            node: Current node in traversal

        Returns:
            New BinaryNodeRef with min node removed
        """
        if self._follow(node.left_ref) is None:
            return node.right_ref
        else:
            return self.node_ref_class(
                referent=BinaryNode.from_node(
                    node, left_ref=self._delete_min(self._follow(node.left_ref))
                )
            )


class BinaryNode:
    """
    Immutable node in the binary tree.
    Contains key, value, and references to left/right children.
    """

    def __init__(self, left_ref, key, value_ref, right_ref, length):
        self.left_ref = left_ref
        self.key = key
        self.value_ref = value_ref
        self.right_ref = right_ref
        self.length = length  # Number of nodes in this subtree

    def store_refs(self, storage):
        """
        Store all references in this node.

        Args:
            storage: Storage object to write to
        """
        self.value_ref.store(storage)
        self.left_ref.store(storage)
        self.right_ref.store(storage)

    @classmethod
    def from_node(cls, node, **kwargs):
        """
        Create new node based on existing node with optional overrides.

        Args:
            node: Existing node to copy
            **kwargs: Attributes to override

        Returns:
            New BinaryNode with specified changes
        """
        return cls(
            left_ref=kwargs.get("left_ref", node.left_ref),
            key=kwargs.get("key", node.key),
            value_ref=kwargs.get("value_ref", node.value_ref),
            right_ref=kwargs.get("right_ref", node.right_ref),
            length=kwargs.get("length", node.length),
        )


class BinaryNodeRef(ValueRef):
    """
    Reference to a BinaryNode in storage.
    Handles serialization/deserialization of nodes.
    """

    def prepare_to_store(self, storage):
        """
        Prepare node for storage by storing all its references.

        Args:
            storage: Storage object to write to
        """
        if self._referent:
            self._referent.store_refs(storage)

    @staticmethod
    def referent_to_string(referent):
        """
        Serialize node to string for storage.

        Args:
            referent: BinaryNode to serialize

        Returns:
            Pickled string representation
        """
        return pickle.dumps(
            {
                "left": referent.left_ref.address,
                "key": referent.key,
                "value": referent.value_ref.address,
                "right": referent.right_ref.address,
                "length": referent.length,
            }
        )

    @staticmethod
    def string_to_referent(string):
        """
        Deserialize node from string.

        Args:
            string: Pickled string representation

        Returns:
            Reconstructed BinaryNode
        """
        d = pickle.loads(string)
        return BinaryNode(
            BinaryNodeRef(address=d["left"]),
            d["key"],
            ValueRef(address=d["value"]),
            BinaryNodeRef(address=d["right"]),
            d["length"],
        )


# Set the reference class after all classes are defined
BinaryTree.node_ref_class = BinaryNodeRef
