import dbdb
import sys


def main(argv):
    if len(argv) < 4 or len(argv) > 5:
        print(
            "Usage: python -m dbdb.tool <database_name> <set|get|find|remove> <key> [<value>]"
        )
        sys.exit(1)

    dbname = argv[1]
    verb = argv[2]
    key = argv[3]

    # Connect to the database
    db = dbdb.connect(dbname)

    try:
        if verb == "set":
            if len(argv) != 5:
                print("Usage: python -m dbdb.tool <database_name> set <key> <value>")
                sys.exit(1)
            value = argv[4]
            db[key] = value  # Set the value for the key
            db.commit()  # Commit the transaction
            print(f"Successfully set {key} to {value} in {dbname}.")

        elif verb == "get":
            # Get the value for the key
            if key in db:
                print(f"The value for {key} is {db[key]} in {dbname}.")
            else:
                print(f"Error: The key '{key}' does not exist in {dbname}.")
                sys.exit(1)

        elif verb == "find":
            # Find the key (check if it exists)
            if key in db:
                print(f"Found {key} in {dbname}.")
            else:
                print(f"Error: The key '{key}' does not exist in {dbname}.")
                sys.exit(1)

        elif verb == "remove":
            # Remove the key from the database
            if key in db:
                del db[key]  # Remove the key
                db.commit()  # Commit the transaction
                print(f"Successfully removed {key} from {dbname}.")
            else:
                print(f"Error: The key '{key}' does not exist in {dbname}.")
                sys.exit(1)

        else:
            print(f"Unsupported verb: {verb}")
            sys.exit(1)

    except KeyError:
        print(f"Error: The key '{key}' does not exist.")
        sys.exit(1)

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
