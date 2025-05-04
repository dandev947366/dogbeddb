# storage.py
import os
import pickle


class Storage:
    """Handles low-level file operations"""

    def __init__(self, fileobj):
        self._file = fileobj
        self.closed = False

    def write(self, data: bytes) -> int:
        """Write data to storage"""
        self._file.seek(0, os.SEEK_END)
        address = self._file.tell()
        self._file.write(data)
        return address

    def read(self, address: int) -> bytes:
        """Read data from storage"""
        self._file.seek(address)
        return self._file.read()

    def close(self):
        """Close the storage file"""
        if not self.closed:
            self._file.close()
            self.closed = True

    def flush(self):
        """Flush writes to disk"""
        self._file.flush()

    def lock(self):
        """Prevent other processes from modifying the file."""
        import fcntl

        fcntl.flock(self.file.fileno(), fcntl.LOCK_EX)
        return True

    def set(self, address, node_ref):
        """Wrapper to match binary_tree's expected interface"""
        data = pickle.dumps(node_ref)
        if address is None:  # New node
            return self.write(data)
        else:  # Update existing
            self._file.seek(address)
            self._file.write(data)
            return address

    def commit_root_address(self, address):
        self._file.seek(0)
        self._file.write(address.to_bytes(8, "big"))
