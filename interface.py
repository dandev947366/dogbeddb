from storage import Storage
from binary_tree import BinaryTree


class DBDB(object):
    """
    DBDB - A Python dictionary-like interface for the binary tree database.
    Implements the standard dictionary methods for key/value storage.
    """

    def __init__(self, f):
        """
        Initialize the database with a file object.

        Args:
            f: File object opened in binary read/write mode
        """
        self._storage = Storage(f)  # Physical storage layer
        self._tree = BinaryTree(self._storage)  # Logical binary tree implementation

    def __getitem__(self, key):
        """
        Retrieve a value by key.
        Implements the dict[key] operation.

        Args:
            key: Key to look up

        Returns:
            The value associated with the key

        Raises:
            KeyError: If key doesn't exist
            ValueError: If database is closed
        """
        self._assert_not_closed()
        try:
            return self._tree.get(key)
        except KeyError:
            raise KeyError(f"Key '{key}' not found in the database.")

    def __setitem__(self, key, value):
        """
        Set a key/value pair.
        Implements the dict[key] = value operation.

        Args:
            key: Key to set
            value: Value to associate with key
        """
        self._assert_not_closed()
        self._tree.set(key, value)

    def __delitem__(self, key):
        """
        Delete a key/value pair.
        Implements the del dict[key] operation.

        Args:
            key: Key to delete

        Raises:
            KeyError: If key doesn't exist
        """
        self._assert_not_closed()
        try:
            self._tree.delete(key)
        except KeyError:
            raise KeyError(f"Key '{key}' does not exist in the database.")

    def commit(self):
        """
        Commit pending changes to disk.
        Ensures all operations are durable.
        """
        self._assert_not_closed()
        try:
            self._tree.commit()
        except Exception as e:
            raise Exception(f"Error committing changes: {e}")

    def _assert_not_closed(self):
        """
        Internal method to verify database is open.

        Raises:
            ValueError: If database is closed
        """
        if self._storage.closed:
            raise ValueError("Database is closed.")

    def close(self):
        """
        Close the database and release resources.
        """
        if hasattr(self, "_storage"):
            self._storage.close()

    def __contains__(self, key):
        """
        Test for key membership.
        Implements the 'key in dict' operation.
        """
        try:
            self[key]
            return True
        except KeyError:
            return False

    def __enter__(self):
        """Support for context manager protocol (with statement)"""
        return self

    def __exit__(self, *exc_info):
        """Support for context manager protocol (with statement)"""
        self.close()

    def __getstate__(self):
        """Prepare the state for serialization (pickling)"""
        return self._tree  # Serialize the tree structure

    def __setstate__(self, state):
        """Restore the state from deserialization"""
        self._tree = state
        self._storage = Storage(
            self._tree.storage_file
        )  # Reinitialize the storage with the restored tree
