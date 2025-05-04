import abc

class LogicalBase(metaclass=abc.ABCMeta):
    """
    Abstract base class defining the logical interface for the key-value store.
    Handles dereferencing, storage locking, and tree management.
    """
    
    def __init__(self, storage):
        self._storage = storage
        self._tree_ref = None  # Will be initialized by concrete subclass
        self._refresh_tree_ref()

    def get(self, key):
        """
        Retrieve a value by key.
        
        Args:
            key: Key to look up
            
        Returns:
            The value associated with the key
            
        Raises:
            KeyError: If key doesn't exist
        """
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._get(self._follow(self._tree_ref), key)

    def set(self, key, value):
        """
        Set a key/value pair.
        
        Args:
            key: Key to set
            value: Value to associate with key
            
        Returns:
            The new tree root reference
        """
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._insert(
            self._follow(self._tree_ref), key, self.value_ref_class(value))
        return self._tree_ref

    def delete(self, key):
        """
        Delete a key/value pair.
        
        Args:
            key: Key to delete
            
        Returns:
            The new tree root reference
            
        Raises:
            KeyError: If key doesn't exist
        """
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._delete(self._follow(self._tree_ref), key)
        return self._tree_ref

    def commit(self):
        """
        Commit pending changes to storage.
        """
        self._tree_ref.store(self._storage)
        self._storage.commit_root_address(self._tree_ref.address)

    def _refresh_tree_ref(self):
        """
        Refresh the tree reference from storage.
        """
        self._tree_ref = self.node_ref_class(
            address=self._storage.get_root_address())

    def _follow(self, ref):
        """
        Dereference a reference to get its referent.
        
        Args:
            ref: Reference to dereference
            
        Returns:
            The referenced object
        """
        return ref.get(self._storage)

    @abc.abstractmethod
    def _get(self, node, key):
        """Concrete implementation must implement key lookup"""
        raise NotImplementedError

    @abc.abstractmethod
    def _insert(self, node, key, value_ref):
        """Concrete implementation must implement insertion"""
        raise NotImplementedError

    @abc.abstractmethod
    def _delete(self, node, key):
        """Concrete implementation must implement deletion"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def value_ref_class(self):
        """Concrete implementation must specify value reference class"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def node_ref_class(self):
        """Concrete implementation must specify node reference class"""
        raise NotImplementedError


class ValueRef(object):
    """
    Reference to a binary blob stored in the database.
    Provides indirection to avoid loading entire database into memory.
    """
    
    def __init__(self, referent=None, address=None):
        self._referent = referent  # Python object being referenced
        self._address = address    # Storage address of the object

    def prepare_to_store(self, storage):
        """
        Prepare referenced object for storage.
        """
        pass

    def store(self, storage):
        """
        Store referenced object if not already stored.
        
        Args:
            storage: Storage object to write to
        """
        if self._referent is not None and not self._address:
            self.prepare_to_store(storage)
            self._address = storage.write(self.referent_to_string(self._referent))

    @property
    def address(self):
        """
        Get storage address of referenced object.
        """
        return self._address

    @classmethod
    def referent_to_string(cls, referent):
        """
        Convert referent to string for storage.
        
        Args:
            referent: Object to serialize
            
        Returns:
            Serialized representation of object
        """
        raise NotImplementedError

    @classmethod
    def string_to_referent(cls, string):
        """
        Convert string back to referent.
        
        Args:
            string: Serialized representation
            
        Returns:
            Deserialized object
        """
        raise NotImplementedError

    def get(self, storage):
        """
        Get referenced object, loading from storage if necessary.
        
        Args:
            storage: Storage object to read from
            
        Returns:
            The referenced object
        """
        if self._referent is None and self._address:
            self._referent = self.string_to_referent(storage.read(self._