"""An unresolved stored signature fails before evaluating any argument."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("arity_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checked = 0
    failures = []
    created = []

    def check(sql, rows=None, state=None, describe=False):
        nonlocal checked
        checked += 1
        if describe:
            parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] == state and (rows is None or result[0] == rows)
        if state:
            valid = valid and result[0] == [] and result[4] is None
            if describe:
                valid = valid and not any(kind in {b"1", b"T", b"D", b"C"} for kind, _ in messages)
        print("STORED_ARITY", sql, "describe=", describe, result, "pass=", valid, flush=True)
        if not valid:
            failures.append((sql, describe, result, rows, state))
        return result

    try:
        declarations = [
            ("TABLE stored_arity_effects", "CREATE TABLE stored_arity_effects(id INT)"),
            ("FUNCTION stored_arity_add(INT)", "CREATE FUNCTION stored_arity_add(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x+1 $$"),
            ("FUNCTION stored_arity_zero()", "CREATE FUNCTION stored_arity_zero() RETURNS INT LANGUAGE sql AS $$ SELECT 9 $$"),
            ("FUNCTION stored_arity_writer(INT)", "CREATE FUNCTION stored_arity_writer(x INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO stored_arity_effects VALUES(x); RETURN x; END $$"),
        ]
        for target, sql in declarations:
            result = check(sql)
            if result[1] is None:
                created.append(target)
            else:
                raise AssertionError((sql, result))
        check("SELECT stored_arity_add(1),stored_arity_zero()", [["2", "9"]])
        for expression in [
            "stored_arity_add()", "stored_arity_add(1,2)", "stored_arity_zero(1)",
            "stored_arity_add(1/0,2)", "stored_arity_zero(1/0)",
            "stored_arity_add(stored_arity_writer(7),2)",
            "stored_arity_writer(8),stored_arity_add()",
        ]:
            for suffix in ["", " WHERE false"]:
                sql = "SELECT " + expression + suffix
                check(sql, state="42883")
                check(sql, state="42883", describe=True)
                check("SELECT count(*) FROM stored_arity_effects", [["0"]])
        check("SELECT stored_arity_writer(11)", [["11"]])
        check("SELECT id FROM stored_arity_effects", [["11"]])
        check("SELECT stored_arity_add(NULL)", [[None]])
        print("STORED_ARITY_COMPLETE", checked, "FAILED", len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if reference:
            try:
                for target in reversed(created):
                    client.simple_query(sock, "DROP " + target)
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
