import pickle  # For serializing and deserializing Python objects
from typing import Optional, Any
from storage import Storage


class Noderef:
    """Reference to a serialized node stored on disk"""

    def __init__(self, address: Optional[int] = None, node: Optional["Node"] = None):
        self.address = address  # Address of the node in the file (if stored)
        self._node = node  # In-memory instance of the node (optional)

    def get(self, storage: Storage) -> "Node":
        """Load the node from disk if not already loaded in memory"""
        if self._node is None and self.address is not None:
            data = storage.read(self.address)  # Read serialized data from disk
            self._node = pickle.loads(data)  # Deserialize it into a Node
        return self._node  # Return the loaded node

    def store(self, storage: Storage) -> int:
        """Store the node in the file if not already stored, and return its address"""
        if self._node is not None and self.address is None:
            data = pickle.dumps(self._node)  # Serialize the node into bytes
            self.address = storage.write(data)  # Write to storage and store its address
        return self.address  # Return the address of the node in the file


class Node:
    """Immutable binary tree node structure"""

    __slots__ = ("key", "value_ref", "left_ref", "right_ref", "length")
    # Using __slots__ to save memory and prevent dynamic attribute creation

    def __init__(
        self,
        key: Any,  # The key associated with this node (can be any type)
        value_ref: Noderef,  # Reference to the value associated with the key
        left_ref: Optional[Noderef] = None,  # Optional reference to the left child
        right_ref: Optional[Noderef] = None,  # Optional reference to the right child
    ):
        self.key = key  # Assign the key
        self.value_ref = value_ref  # Assign the value reference
        self.left_ref = left_ref or Noderef()  # Use empty reference if not provided
        self.right_ref = right_ref or Noderef()  # Corrected typo from right_left
        self.length = (
            1 + self.left_ref.get(storage=None).length
            if self.left_ref.address
            else (
                0 + self.right_ref.get(storage=None).length
                if self.right_ref.address
                else 0
            )
        )
        # Compute the size of the subtree (naively here; fix depends on tree logic)

    def store_refs(self, storage: Storage):
        """Recursively store this node's references (value, left, right)"""
        self.value_ref.store(storage)  # Store value node
        self.left_ref.store(storage)  # Store left child node
        self.right_ref.store(storage)  # Store right child node
