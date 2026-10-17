#!/usr/bin/env python3
"""Direct physical projections retain typmods across scalar type aliases."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("type_alias_runner", root / "tests/compat/pg_diff_runner.py")
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
    controls, failures = 0, []
    fields = [("c", 1042, -1, 7, 1), ("v", 1043, -1, 9, 2),
              ("b", 1560, -1, 4, 3), ("bv", 1562, -1, 6, 4),
              ("n", 1700, -1, (8 << 16) + 6, 5), ("id", 23, 4, -1, 6)]

    def setup(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def check(mode, sql, expected, rows):
        nonlocal controls
        controls += 1
        if mode == "simple":
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
            if mode == "describe":
                packet += client.typed(b"D", b"S\0")
            else:
                packet += client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
                packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0\0\0\0\0")
            sock.sendall(packet + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        actual = client.row_description_fields(messages) if any(k == b"T" for k, _ in messages) else []
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]
        metadata = [(f[0].decode(), f[3], f[4], f[5], f[2]) for f in actual]
        desired = [(name, oid, length, modifier, attribute) for name, oid, length, modifier, attribute in expected]
        origin = len(actual) == len(expected) and all(
            (field[1] > 0 if expected[i][4] else field[1] == 0) and field[6] == 0
            for i, field in enumerate(actual))
        values = client.data_row_values(messages)
        tags = [body.rstrip(b"\0").decode() for kind, body in messages if kind == b"C"]
        good = not errors and metadata == desired and origin
        good = good and (not values and not tags if mode == "describe" else
                         values == rows and tags == ["SELECT " + str(len(rows))])
        print("TYPE_ALIAS_METADATA", controls, mode, sql, "pass", good, "metadata", metadata, "errors", errors, flush=True)
        if not good:
            failures.append((mode, sql, metadata, desired, values, rows, tags, errors, origin))

    def phase(table, rows, array=False):
        shape = fields + ([("a", 1007, -1, -1, 7)] if array else [])
        cases = [
            ("SELECT * FROM alias_metadata_rows", shape, rows),
            ("SELECT c AS v,v AS c FROM alias_metadata_rows",
             [("v", 1042, -1, 7, 1), ("c", 1043, -1, 9, 2)], [r[:2] for r in rows]),
            ("SELECT t.v AS c,t.c AS v FROM alias_metadata_rows AS t",
             [("c", 1043, -1, 9, 2), ("v", 1042, -1, 7, 1)], [[r[1], r[0]] for r in rows]),
            ("SELECT * FROM alias_metadata_rows WHERE false", shape, []),
            ("SELECT 1 AS c,NULL::text AS v FROM alias_metadata_rows",
             [("c", 23, 4, -1, 0), ("v", 25, -1, -1, 0)], [[b"1", None] for _ in rows]),
            ("SELECT CAST(c AS text) AS c,CAST(v AS text) AS v FROM alias_metadata_rows WHERE false",
             [("c", 25, -1, -1, 0), ("v", 25, -1, -1, 0)], []),
        ]
        for sql, expected, values in cases:
            sql = sql.replace("alias_metadata_rows", table)
            for mode in ("simple", "describe", "execute"):
                check(mode, sql, expected, values)

    try:
        if reference:
            setup("BEGIN")
            schema = "alias_metadata_" + uuid.uuid4().hex[:12]
            setup("CREATE SCHEMA " + schema)
            setup("SET search_path TO " + schema)
        for prefix, table, array in [("TEMP ", "alias_metadata_temp", False), ("", "alias_metadata_rows", False),
                                     ("TEMP ", "alias_metadata_temp_array", True), ("", "alias_metadata_array", True)]:
            setup("CREATE " + prefix + "TABLE " + table + "(c CHAR(3),v VARCHAR(5),b BIT(4),bv VARBIT(6),n NUMERIC(8,2),id INTEGER" +
                  (",a INTEGER[]" if array else "") + ")")
            phase(table, [], array)
            setup("INSERT INTO " + table + " DEFAULT VALUES")
            phase(table, [[None] * (7 if array else 6)], array)
            setup("DELETE FROM " + table)
            setup("INSERT INTO " + table + " VALUES('ab','xyz',B'1010',B'101',12.30,7" +
                  (",ARRAY[1,NULL]" if array else "") + ")")
            phase(table, [[b"ab ", b"xyz", b"1010", b"101", b"12.30", b"7"]] if not array else
                  [[b"ab ", b"xyz", b"1010", b"101", b"12.30", b"7", b"{1,NULL}"]], array)
        setup("CREATE TABLE alias_metadata_mixed(d DATE,t TIME,z TIMETZ,i INTERVAL,p INTERVAL DAY TO SECOND(3),"
              "c CHAR(3),n NUMERIC(8,2),a INTEGER[],ia INTERVAL[],g POINT,ln LINE,u UUID,da DATE[],ta TIME[])")
        mixed = [("d", 1082, 4, -1, 1), ("t", 1083, 8, -1, 2), ("z", 1266, 12, -1, 3),
                 ("i", 1186, 16, -1, 4), ("p", 1186, 16, (7176 << 16) | 3, 5),
                 ("c", 1042, -1, 7, 6), ("n", 1700, -1, (8 << 16) + 6, 7),
                 ("a", 1007, -1, -1, 8), ("ia", 1187, -1, -1, 9), ("g", 600, 16, -1, 10),
                 ("ln", 628, 24, -1, 11), ("u", 2950, 16, -1, 12),
                 ("da", 1182, -1, -1, 13), ("ta", 1183, -1, -1, 14)]
        for sql, expected in [("SELECT c FROM alias_metadata_mixed WHERE false", [mixed[5]]),
                              ("SELECT c AS p FROM alias_metadata_mixed WHERE false", [("p",) + mixed[5][1:]]),
                              ("SELECT * FROM alias_metadata_mixed WHERE false", mixed),
                              ("SELECT CAST(c AS text) AS c FROM alias_metadata_mixed WHERE false", [("c", 25, -1, -1, 0)])]:
            for mode in ("simple", "describe", "execute"):
                check(mode, sql, expected, [])
        assert controls == 228, controls
        assert not failures, failures
        print("[TYPE ALIAS PROJECTION METADATA] all 228 controls passed", flush=True)
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
