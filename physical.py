import os
from .interface import DBDB


def connect(dbname):
    """Open or create a database file and return a DBDB instance.

    Args:
        dbname: Path to the database file

    Returns:
        DBDB: An instance of the database interface
    """
    try:
        # Try to open in read/write binary mode
        f = open(dbname, "r+b")
    except IOError:
        # If file doesn't exist, create it
        fd = os.open(dbname, os.O_RDWR | os.O_CREAT)
        f = os.fdopen(fd, "r+b")
    return DBDB(f)


# Make the DBDB class available at package level
__all__ = ["DBDB", "connect"]
