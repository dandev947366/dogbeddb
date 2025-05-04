import os  # Import the os module for file handling and low-level file operations.
import portalocker  # Import the portalocker module for file locking to ensure exclusive access to the file.


class Storage:
    """Handles low-level file operations with locking"""

    def __init__(self, filename: str):
        """
        Initializes the Storage object.
        Args:
            filename (str): The name of the file that will be used for storage.
        """
        self.filename = filename  # Store the filename.
        self.file = self._open_file()  # Open the file or create it if it doesn't exist.
        self.locked = False  # Initially, the file is not locked.

    def _open_file(self):
        """
        Opens the file for read/write access, or creates the file if it doesn't exist.
        Returns:
            file object: The opened file object.
        """
        try:
            # Attempt to open the file in read/write binary mode ('r+b').
            f = open(self.filename, "r+b")
        except FileNotFoundError:
            # If the file doesn't exist, create it with read/write access ('r+b').
            fd = os.open(
                self.filename, os.O_RDWR | os.O_CREAT
            )  # Open the file with os-level flags.
            f = os.fdopen(fd, "r+b")  # Convert the file descriptor to a file object.
        return f  # Return the opened file object.

    def lock(self) -> bool:
        """
        Acquires an exclusive lock on the file to ensure no other processes can access it.
        Returns:
            bool: True if the lock was successfully acquired, False if the file is already locked.
        """
        if not self.locked:  # Check if the file is not already locked.
            portalocker.lock(
                self.file, portalocker.LOCK_EX
            )  # Lock the file exclusively.
            self.locked = True  # Mark the file as locked.
            return True  # Return True indicating the lock was acquired successfully.
        return False  # Return False if the file is already locked.

    def unlock(self):
        """
        Releases the lock on the file, allowing other processes to access it.
        """
        if self.locked:  # Check if the file is currently locked.
            portalocker.unlock(self.file)  # Release the lock.
            self.locked = False  # Mark the file as unlocked.

    def write(self, data: bytes) -> int:
        """
        Appends data to the file and returns the address where the data was written.
        Args:
            data (bytes): The data to write to the file.
        Returns:
            int: The address (position) where the data was written in the file.
        """
        self.file.seek(0, os.SEEK_END)  # Move the file pointer to the end of the file.
        address = (
            self.file.tell()
        )  # Get the current position of the file pointer (address).
        self.file.write(data)  # Write the data at the current file pointer position.
        return address  # Return the address where the data was written.

    def read(self, address: int) -> bytes:
        """
        Reads data from a specific address in the file.
        Args:
            address (int): The address (position) to start reading from.
        Returns:
            bytes: The data read from the file.
        """
        self.file.seek(address)  # Move the file pointer to the specified address.
        length = int.from_bytes(
            self.file.read(4), "big"
        )  # Read the first 4 bytes to determine the length of the data.
        return self.file.read(
            length
        )  # Read and return the data of the specified length.

    def commit_root_address(self, address: int):
        """
        Atomically updates the root pointer in the file (at the beginning).
        Args:
            address (int): The new root address to store in the file.
        """
        self.file.seek(0)  # Move the file pointer to the beginning of the file.
        self.file.write(
            address.to_bytes(8, "big")
        )  # Write the root address as an 8-byte big-endian integer.
        self.file.flush()  # Ensure the changes are written to disk immediately (flush the buffer).
