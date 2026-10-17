"""Real array row declarations, not brace-looking TEXT; PostgreSQL 18 oracle."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "array_inspection_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checks, failures = 0, []
    table = "array_inspection_typed_rows"

    def check(sql, rows=None, oid=None, state=None, describe=False):
        nonlocal checks
        checks += 1
        if describe:
            parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            sock.sendall(client.typed(b"P", parse) +
                         client.typed(b"D", b"S\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        good = result[1] == state and messages[-1] == (b"Z", b"I")
        good = good and (rows is None or result[0] == rows)
        good = good and (oid is None or result[5] == [oid])
        if describe:
            good = good and result[0] == [] and result[4] is None
            good = good and sum(tag == b"1" for tag, _ in messages) == (0 if state else 1)
        if state:
            good = good and result[0] == [] and result[4] is None
            good = good and sum(tag == b"E" for tag, _ in messages) == 1
            good = good and not any(tag in (b"D", b"C") for tag, _ in messages)
            if state == "42883":
                good = good and not any(tag == b"T" for tag, _ in messages)
        if not good:
            failures.append((sql, describe, result, [tag for tag, _ in messages]))
            print("ARRAY_INSPECTION_FAILURE", failures[-1], flush=True)
        return good

    try:
        assert check("CREATE TEMP TABLE " + table +
                     "(flat_array INTEGER[],matrix_array INTEGER[],"
                     "null_element_array INTEGER[],empty_array INTEGER[],"
                     "null_array INTEGER[],text_array TEXT,null_text TEXT)")
        assert check("INSERT INTO " + table +
                     " VALUES('{10,20,30}','{{1,2},{3,4},{5,6}}',"
                     "'{10,NULL,30}','{}',NULL,'{10,20,30}',NULL)")
        cases = [
            ("array_dims(flat_array)", "[1:3]", 25),
            ("array_dims(matrix_array)", "[1:3][1:2]", 25),
            ("cardinality(matrix_array)", "6", 23),
            ("cardinality(empty_array)", "0", 23),
            ("cardinality(null_array)", None, 23),
            ("array_position(flat_array,20)", "2", 23),
            ("array_position(flat_array,99)", None, 23),
            ("array_position(null_element_array,NULL)", "2", 23),
            ("array_position(flat_array,10,-2)", "1", 23),
            ("array_position(empty_array,NULL)", None, 23),
            ("array_position(empty_array,1)", None, 23),
            ("array_position(null_array,1)", None, 23),
            ("array_position(null_array,NULL)", None, 23),
            ("array_position(flat_array,20,3)", None, 23),
            ("array_position(null_array,NULL,NULL::integer)", None, 23),
        ]
        for expression, value, oid in cases:
            check("SELECT " + expression + " FROM " + table, rows=[[value]], oid=oid)
        for expression, state in (
            ("array_position(flat_array,10,2147483648)", "42883"),
            ("array_position(flat_array,10,2147483648::integer)", "22003"),
            ("array_position(null_element_array,NULL,NULL::integer)", "22004"),
            ("array_position(matrix_array,1)", "0A000"),
        ):
            check("SELECT " + expression + " FROM " + table, state=state)
        # A SQL TEXT declaration stays TEXT even for a serialized array or
        # a NULL datum; no row demand may bypass signature admission.
        for column in ("text_array", "null_text"):
            for needle in ("20", "'20'::text"):
                for suffix in ("", " WHERE false", " LIMIT 0"):
                    sql = "SELECT array_position(" + column + "," + needle + ") FROM " + table + suffix
                    check(sql, state="42883")
                    check(sql, state="42883", describe=True)
        for suffix in (" WHERE false", " LIMIT 0"):
            sql = "SELECT array_position(flat_array,20) FROM " + table + suffix
            check(sql, rows=[], oid=23)
            check(sql, rows=[], oid=23, describe=True)
        check("SELECT 42", rows=[["42"]], oid=23)
        check("DROP TABLE " + table)
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)
    print("ARRAY_INSPECTION_CHECKED=", checks, "FAILED=", len(failures), flush=True)
    assert checks == 51 and not failures, (checks, failures)


if __name__ == "__main__":
    main()
