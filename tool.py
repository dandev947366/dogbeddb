import sys
import interface


def usage():
    print("Usage:", file=sys.stderr)
    print(f"  {sys.argv[0]} <filename> get <key>", file=sys.stderr)
    print(f"  {sys.argv[0]} <filename> set <key> <value>", file=sys.stderr)
    print(f"  {sys.argv[0]} <filename> delete <key>", file=sys.stderr)


def main(argv):
    try:
        if len(argv) < 4:
            usage()
            return 1

        dbname, verb, key = argv[1:4]
        value = argv[4] if len(argv) > 4 else None

        if verb not in {"get", "set", "delete"}:
            usage()
            return 1

        if verb == "set" and value is None:
            print("Error: 'set' operation requires a value", file=sys.stderr)
            usage()
            return 1

        with interface.connect(dbname) as db:
            if verb == "get":
                try:
                    print(db[key])
                except KeyError:
                    print(f"Error: Key '{key}' not found", file=sys.stderr)
                    return 1
            elif verb == "set":
                db[key] = value
                db.commit()
                print(f"Set {key} = {value}")
            elif verb == "delete":
                try:
                    del db[key]
                    db.commit()
                    print(f"Deleted key: {key}")
                except KeyError:
                    print(f"Error: Key '{key}' not found", file=sys.stderr)
                    return 1

        return 0
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
