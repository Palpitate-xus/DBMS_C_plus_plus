#!/usr/bin/env python3
"""Physical heap widths must not replace PostgreSQL declared type lengths."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("attribute_length_runner", root / "tests/compat/pg_diff_runner.py")
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
    fields = [("d", "DATE", 1082, 4, -1), ("t", "TIME", 1083, 8, -1),
              ("z", "TIMETZ", 1266, 12, -1), ("i", "INTERVAL", 1186, 16, -1),
              ("p", "INTERVAL DAY TO SECOND(3)", 1186, 16, (7176 << 16) | 3),
              ("c", "CHAR(3)", 1042, -1, 7), ("n", "NUMERIC(8,2)", 1700, -1, (8 << 16) + 6),
              ("a", "INTEGER[]", 1007, -1, -1), ("ia", "INTERVAL[]", 1187, -1, -1),
              ("g", "POINT", 600, 16, -1), ("ln", "LINE", 628, 24, -1), ("u", "UUID", 2950, 16, -1),
              ("da", "DATE[]", 1182, -1, -1), ("ta", "TIME[]", 1183, -1, -1)]

    def simple(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def check(mode, sql, expected, rows, tag="SELECT 0", physical=True):
        nonlocal checks
        checks += 1
        if mode == "simple":
            messages = client.simple_query(sock, sql)
            code = 0
        else:
            parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            if mode == "describe":
                packet = client.typed(b"P", parse) + client.typed(b"D", b"S\0") + client.typed(b"S")
                code = 0
            else:
                bind = b"\0\0" + struct.pack("!HHHH", 0, 0, 1, 1)
                packet = client.typed(b"P", parse) + client.typed(b"B", bind) + client.typed(b"D", b"P\0")
                packet += client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S")
                code = 1
            sock.sendall(packet)
            messages = client.read_until_ready(sock)
        actual = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]
        values = client.data_row_values(messages)
        metadata = [(field[0].decode(), field[3], field[4], field[5], field[6]) for field in actual]
        desired = [(name, oid, length, modifier, code) for name, _, oid, length, modifier in expected]
        tags = [body.rstrip(b"\0").decode() for kind, body in messages if kind == b"C"]
        good = not errors and metadata == desired
        if physical:
            good = good and all(field[1] > 0 and field[2] > 0 for field in actual)
        if mode == "describe":
            good = good and not values and not tags
        else:
            good = good and values == rows and tags == [tag]
        print("PHYSICAL_ATTRIBUTE_LENGTH", checks, mode, sql, "match", good, "metadata", metadata, "errors", errors, flush=True)
        if not good:
            failures.append((mode, sql, metadata, desired, values, rows, tags, errors))

    def phase(table, expected, row_count):
        # Catalog rows and cold/warm upgrade semantics are covered by the
        # native owner test. SQL catalog-source routing is a separate gap.
        check("simple", "SELECT * FROM " + table + " WHERE false", expected, [], "SELECT 0")
        check("describe", "SELECT * FROM " + table, expected, [])
        check("execute", "SELECT * FROM " + table, expected, [[None] * len(expected)] * row_count, "SELECT " + str(row_count))

    try:
        if reference:
            simple("BEGIN")
            schema = "attribute_length_" + uuid.uuid4().hex[:12]
            simple("CREATE SCHEMA " + schema)
            simple("SET search_path TO " + schema)
        simple("CREATE TABLE attribute_length_rows(" + ",".join(name + " " + declaration for name, declaration, *_ in fields) + ")")
        phase("attribute_length_rows", fields, 0)
        check("execute", "INSERT INTO attribute_length_rows DEFAULT VALUES RETURNING *", fields, [[None] * len(fields)], "INSERT 0 1")
        phase("attribute_length_rows", fields, 1)
        simple("ALTER TABLE attribute_length_rows RENAME COLUMN i TO renamed")
        fields[3] = ("renamed",) + fields[3][1:]
        phase("attribute_length_rows", fields, 1)
        aliases = [("d", "INTERVAL", 1186, 16, -1), ("renamed", "DATE", 1082, 4, -1), ("z", "TIMETZ", 1266, 12, -1)]
        sql = "SELECT renamed AS d,d AS renamed,z FROM attribute_length_rows"
        check("simple", sql, aliases, [[None] * 3], "SELECT 1")
        check("describe", sql, aliases, [])
        check("execute", sql, aliases, [[None] * 3], "SELECT 1")
        simple("ALTER TABLE attribute_length_rows ADD COLUMN extra DATE")
        fields.append(("extra", "DATE", 1082, 4, -1))
        phase("attribute_length_rows", fields, 1)
        simple("ALTER TABLE attribute_length_rows ALTER COLUMN p TYPE INTERVAL SECOND(2)")
        fields[4] = ("p", "INTERVAL SECOND(2)", 1186, 16, (4096 << 16) | 2)
        phase("attribute_length_rows", fields, 1)
        simple("CREATE VIEW attribute_length_view AS SELECT * FROM attribute_length_rows")
        # Keep descriptor checking independent of the pre-existing execution
        # failure for views containing NULL DATE/TIME/TIMETZ. Preserve those
        # failures in the review ledger and the original mixed-contract draft.
        check("describe", "SELECT * FROM attribute_length_view", fields, [], physical=False)
        for declaration, oid, length in [("DATE", 1082, 4), ("TIME", 1083, 8), ("TIMETZ", 1266, 12), ("INTERVAL", 1186, 16)]:
            expected = [("v", declaration, oid, length, -1)]
            sql = "SELECT NULL::" + declaration + " AS v"
            check("simple", sql, expected, [[None]], "SELECT 1", physical=False)
            check("describe", sql, expected, [], physical=False)
        assert checks == 28, checks
        assert not failures, ("physical attribute length failures", failures)
        print("[PHYSICAL ATTRIBUTE LENGTH] all 28 controls passed", flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
