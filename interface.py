import os
import dbdb


def connect(dbname):
    """Connect to or create a database file.

    Args:
        dbname: Path to the database file (string)

    Returns:
        DBDB instance
    """
    try:
        # Try to open in read-write mode first
        f = open(dbname, "r+b")
    except FileNotFoundError:
        # Create the file if it doesn't exist
        fd = os.open(dbname, os.O_RDWR | os.O_CREAT, 0o644)  # Added file permissions
        f = os.fdopen(fd, "r+b")
    except PermissionError:
        raise PermissionError(f"Permission denied when accessing {dbname}")
    except Exception as e:
        raise IOError(f"Could not open database file {dbname}: {str(e)}")

    return dbdb.DBDB(f)
