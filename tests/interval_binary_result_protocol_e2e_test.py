#!/usr/bin/env python3
"""PostgreSQL interval_send is int64 time, int32 days, int32 months."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def cases():
    return [
        ("0", 0, 0, 0), ("1 microsecond", 1, 0, 0), ("-1 microsecond", -1, 0, 0),
        ("1 second", 1000000, 0, 0), ("-1 second", -1000000, 0, 0),
        ("123.456789 seconds", 123456789, 0, 0), ("-123.456789 seconds", -123456789, 0, 0),
        ("1 mon 2 days 03:04:05.000006", 11045000006, 2, 1),
        ("-1 mon -2 days -03:04:05.000006", -11045000006, -2, -1),
        ("1 mon -2 days +03:04:05.000006", 11045000006, -2, 1),
        ("-1 mon 2 days -03:04:05.000006", -11045000006, 2, -1),
        ("24 hours", 86400000000, 0, 0), ("1 day", 0, 1, 0), ("1 year", 0, 0, 12),
        ("0.9 years", 0, 0, 11), ("1.5 months", 0, 15, 1), ("-1.5 months", 0, -15, -1),
        ("1.5 weeks", 43200000000, 10, 0), ("1.5 days", 43200000000, 1, 0),
        ("1.5 microseconds", 1, 0, 0), ("00:00:00.0000015", 2, 0, 0),
        ("P1Y2M3DT4H5M6.000007S", 14706000007, 3, 14),
        ("@ 1 day 2 hours ago", -7200000000, -1, 0), ("1-2", 0, 0, 14), ("-0-2", 0, 0, -2),
        ("2147483647 months", 0, 0, 2**31 - 1), ("-2147483648 months", 0, 0, -(2**31)),
        ("2147483647 days", 0, 2**31 - 1, 0), ("-2147483648 days", 0, -(2**31), 0),
        ("9223372036854775807 microseconds", 2**63 - 1, 0, 0),
        ("-9223372036854775808 microseconds", -(2**63), 0, 0),
        ("178956971 years -5 months", 0, 0, 2**31 - 1),
        ("infinity", 2**63 - 1, 2**31 - 1, 2**31 - 1),
        ("-infinity", -(2**63), -(2**31), -(2**31)),
        ("+infinity", 2**63 - 1, 2**31 - 1, 2**31 - 1),
    ]


def interval(micros, days=0, months=0):
    return struct.pack("!qii", micros, days, months)


def array(elements, dimensions=None):
    if dimensions is None:
        dimensions = [(len(elements), 1)] if elements else []
    result = struct.pack("!iii", len(dimensions), int(None in elements), 1186)
    for length, lower in dimensions:
        result += struct.pack("!ii", length, lower)
    for value in elements:
        result += struct.pack("!i", -1 if value is None else len(value)) + (value or b"")
    return result


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("interval_binary_runner", root / "tests/compat/pg_diff_runner.py")
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

    def simple(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def parse(sql, oids):
        return b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(oids)) + b"".join(struct.pack("!I", oid) for oid in oids)

    def bind(values, formats):
        packet = b"\0\0" + struct.pack("!HH", 0, len(values))
        for value in values:
            raw = None if value is None else value.encode()
            packet += struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        return packet + struct.pack("!H", len(formats)) + b"".join(struct.pack("!H", code) for code in formats)

    def check(sql, oids, values, result_oids, expected, tag="SELECT 1", formats=None, modifiers=None):
        nonlocal checks
        checks += 1
        formats = [1] * len(result_oids) if formats is None else formats
        modifiers = [-1] * len(result_oids) if modifiers is None else modifiers
        sock.sendall(client.typed(b"P", parse(sql, oids)) + client.typed(b"B", bind(values, formats)) +
                     client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b"\0").decode() for kind, body in messages if kind == b"C"]
        metadata = [(field[3], field[4], field[5], field[6]) for field in fields]
        desired = [(oid, 16 if oid == 1186 else -1, modifier, code)
                   for oid, modifier, code in zip(result_oids, modifiers, formats)]
        good = (not errors and rows == expected and metadata == desired and tags == [tag] and
                sum(kind == b"1" for kind, _ in messages) == 1 and
                sum(kind == b"2" for kind, _ in messages) == 1 and messages[-1][0] == b"Z")
        print("INTERVAL_BINARY_RESULT", checks, sql, oids, "match", good, "errors", errors, flush=True)
        if not good:
            failures.append((sql, oids, rows, expected, metadata, desired, tags, errors))

    def describe(sql, oids, result_oids):
        nonlocal checks
        checks += 1
        sock.sendall(client.typed(b"P", parse(sql, oids)) + client.typed(b"D", b"S\0") + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        parameters = [body for kind, body in messages if kind == b"t"]
        wanted = struct.pack("!H", len(oids)) + b"".join(struct.pack("!I", oid) for oid in oids)
        good = (parameters == [wanted] and [field[3:] for field in fields] ==
                [(oid, 16 if oid == 1186 else -1, -1, 0) for oid in result_oids] and
                not any(kind in (b"D", b"E", b"C") for kind, _ in messages))
        print("INTERVAL_BINARY_DESCRIBE", checks, sql, "match", good, flush=True)
        if not good:
            failures.append((sql, parameters, fields))

    def rejected(value, state):
        nonlocal checks
        checks += 1
        if reference:
            simple("SAVEPOINT invalid_interval")
        sock.sendall(client.typed(b"P", parse("SELECT $1 AS v", [1186])) +
                     client.typed(b"B", bind([value], [1])) + client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        states = [client.diagnostic_fields(body).get(b"C", b"").decode() for kind, body in messages if kind == b"E"]
        good = states == [state] and not any(kind in (b"D", b"C") for kind, _ in messages)
        print("INTERVAL_BINARY_INPUT_ERROR", checks, value, states, "match", good, flush=True)
        if not good:
            failures.append((value, states, state))
        if reference:
            simple("ROLLBACK TO SAVEPOINT invalid_interval")
            simple("RELEASE SAVEPOINT invalid_interval")

    try:
        if reference:
            simple("BEGIN")
            schema = "interval_binary_" + uuid.uuid4().hex[:12]
            simple("CREATE SCHEMA " + schema)
            simple("SET search_path TO " + schema)
            simple("SET IntervalStyle TO postgres")
        assert len(cases()) == 35
        for value, micros, days, months in cases():
            payload = interval(micros, days, months)
            check("SELECT $1 AS v", [1186], [value], [1186], [[payload]])
            check("SELECT $1 AS v", [1187], ['{"' + value + '",NULL}'], [1187], [[array([payload, None])]])
            literal = value.replace("'", "''")
            check("SELECT INTERVAL '" + literal + "' AS v", [], [], [1186], [[payload]])
            check("SELECT ARRAY[INTERVAL '" + literal + "',NULL::interval] AS v", [], [], [1187], [[array([payload, None])]])
        check("SELECT $1 AS v", [1186], [None], [1186], [[None]])
        check("SELECT NULL::interval AS v", [], [], [1186], [[None]])
        check("SELECT $1 AS v", [1187], [None], [1187], [[None]])
        check("SELECT NULL::interval[] AS v", [], [], [1187], [[None]])
        check("SELECT $1 AS v", [1187], ["{}"], [1187], [[array([])]])
        check("SELECT $1 AS v", [1187], ["{NULL,NULL}"], [1187], [[array([None, None])]])
        check("SELECT $1 AS v", [1187], ['[-3:-1]={"-1 microsecond",NULL,infinity}'], [1187],
              [[array([interval(-1), None, interval(2**63 - 1, 2**31 - 1, 2**31 - 1)], [(3, -3)])]])
        check("SELECT $1 AS v", [1187], ['{{"1 day",NULL},{"24 hours","-1 microsecond"}}'], [1187],
              [[array([interval(0, 1), None, interval(86400000000), interval(-1)], [(2, 1), (2, 1)])]])
        check("SELECT $1 AS v", [1187], ['{{{{{{"1 day"}}}}}}'], [1187], [[array([interval(0, 1)], [(1, 1)] * 6)]])
        check("SELECT $1 AS s,$2 AS a WHERE false", [1186, 1187], ["1 day", '{"1 day"}'], [1186, 1187], [], "SELECT 0")
        for value, expected in [("0", "00:00:00"), ("1 day", "1 day"), ("-1 microsecond", "-00:00:00.000001"), ("infinity", "infinity")]:
            check("SELECT $1 AS v", [1186], [value], [1186], [[expected.encode()]], formats=[0])
        check("SELECT $1 AS v", [1187], ['{"1 day",NULL}'], [1187], [[b'{"1 day",NULL}']], formats=[0])
        for sql, oids, outputs in [("SELECT $1 AS v", [1186], [1186]), ("SELECT $1 AS v", [1187], [1187]),
                                    ("SELECT $1 AS s,$2 AS a", [1186, 1187], [1186, 1187]),
                                    ("SELECT NULL::interval AS v", [], [1186])]:
            describe(sql, oids, outputs)
        simple("CREATE TABLE interval_binary_rows(id INTEGER PRIMARY KEY,s INTERVAL,a INTERVAL[],p INTERVAL DAY TO SECOND(3))")
        typed = [(7176 << 16) | 3]
        payload = interval(3456789, 1)
        bounded = array([interval(0, 0, 1), None, interval(-1)], [(3, -2)])
        args = ["1 day 00:00:03.456789", '[-2:0]={"1 mon",NULL,"-1 microsecond"}']
        expected = [[payload, bounded, interval(3457000, 1)]]
        check("INSERT INTO interval_binary_rows VALUES(1,$1,$2,$1) RETURNING s,a,p", [1186, 1187], args,
              [1186, 1187, 1186], expected, "INSERT 0 1", modifiers=[-1, -1] + typed)
        check("SELECT s,a,p FROM interval_binary_rows WHERE id=1", [], [], [1186, 1187, 1186], expected, modifiers=[-1, -1] + typed)
        changed = [[interval(-(2**63)), array([interval(2**63 - 1, 2**31 - 1, 2**31 - 1)]), interval(3457000, 1)]]
        check("UPDATE interval_binary_rows SET s=$1,a=$2 WHERE id=1 RETURNING s,a,p", [1186, 1187],
              ["-9223372036854775808 microseconds", "{infinity}"], [1186, 1187, 1186], changed, "UPDATE 1", modifiers=[-1, -1] + typed)
        check("SELECT s AS a,a AS s,p FROM interval_binary_rows WHERE id=1", [], [], [1186, 1187, 1186], changed, modifiers=[-1, -1] + typed)
        check("INSERT INTO interval_binary_rows VALUES(2,NULL,NULL,NULL) RETURNING a,s,p", [], [],
              [1187, 1186, 1186], [[None, None, None]], "INSERT 0 1", modifiers=[-1, -1] + typed)
        check("SELECT a,s,p FROM interval_binary_rows WHERE id=2", [], [], [1187, 1186, 1186], [[None, None, None]], modifiers=[-1, -1] + typed)
        check("SELECT a,s,p FROM interval_binary_rows WHERE false", [], [], [1187, 1186, 1186], [], "SELECT 0", modifiers=[-1, -1] + typed)
        empty = [[interval(0), array([]), None]]
        check("INSERT INTO interval_binary_rows VALUES(3,$1,$2,NULL) RETURNING s,a,p", [1186, 1187], ["0", "{}"],
              [1186, 1187, 1186], empty, "INSERT 0 1", modifiers=[-1, -1] + typed)
        check("SELECT s,a,p FROM interval_binary_rows WHERE id=3", [], [], [1186, 1187, 1186], empty, modifiers=[-1, -1] + typed)
        for value, state in [("bad", "22007"), ("2147483648 days", "22015"), ("178956971 years", "22008")]:
            rejected(value, state)
        assert checks == 171, checks
        assert not failures, ("interval binary result failures", failures)
        print("[INTERVAL BINARY RESULT] all 171 controls passed", flush=True)
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
