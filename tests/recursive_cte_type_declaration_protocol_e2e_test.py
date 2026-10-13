#!/usr/bin/env python3
"""Recursive declarations are checked before rows/effects, including empty arms."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("recursive_type_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server["sock"]
    failures = []
    controls = 0
    schema = "recursive_type_" + uuid.uuid4().hex[:16]
    domain = schema + ".int_domain"

    def query(sql, rows=None, oids=None, state=None):
        nonlocal controls
        controls += 1
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == "SELECT " + str(len(rows))
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and result[0] == [] and result[4] is None
        print("RECURSIVE_TYPE", sql, result, "match", valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, oids, state))
        return result

    def parse_reject(sql, parameter_oids=()):
        nonlocal controls
        controls += 1
        name = "recursive_type_" + str(controls)
        body = name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(parameter_oids))
        body += b"".join(struct.pack("!I", oid) for oid in parameter_oids)
        sock.sendall(c.typed(b"P", body) + c.typed(b"S"))
        messages = c.read_until_ready(sock)
        states = [c.diagnostic_fields(body).get(b"C", b"").decode() for kind, body in messages if kind == b"E"]
        valid = states == ["42804"] and not any(kind in (b"1", b"D", b"C") for kind, _ in messages)
        print("RECURSIVE_TYPE_PARSE", sql, states, "match", valid, flush=True)
        if not valid:
            failures.append((sql, states, messages))

    def parse_describe(sql, expected, parameter_oids=()):
        nonlocal controls
        controls += 1
        name = ("recursive_good_" + str(controls)).encode()
        body = name + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(parameter_oids))
        body += b"".join(struct.pack("!I", oid) for oid in parameter_oids)
        sock.sendall(c.typed(b"P", body) + c.typed(b"D", b"S" + name + b"\0") + c.typed(b"S"))
        messages = c.read_until_ready(sock)
        fields = c.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        actual = [(field[3], field[5]) for field in fields]
        valid = actual == expected and sum(kind == b"1" for kind, _ in messages) == 1
        valid = valid and not any(kind in (b"D", b"C", b"E") for kind, _ in messages)
        print("RECURSIVE_TYPE_DESCRIBE", sql, actual, "match", valid, flush=True)
        if not valid:
            failures.append((sql, actual, messages))

    def recursive(seed, arm, predicate="false"):
        return "WITH RECURSIVE q(n) AS (SELECT " + seed + " UNION ALL SELECT " + arm + " FROM q WHERE " + predicate + ") SELECT n FROM q"

    try:
        query("CREATE TEMP SEQUENCE recursive_type_calls")
        query("CREATE TEMP TABLE recursive_type_source(v VARCHAR(1))")
        query("CREATE SCHEMA " + schema)
        query("CREATE DOMAIN " + domain + " AS INTEGER")
        enum_type = schema + ".text"
        query("CREATE TYPE " + enum_type + " AS ENUM('a','b')")
        enum_info = query("SELECT '" + enum_type + "'::regtype::oid", oids=[26])
        assert enum_info[1] is None and len(enum_info[0]) == 1, enum_info
        enum_oid = int(enum_info[0][0][0])
        query("INSERT INTO recursive_type_source VALUES('a')")
        invalid = [
            recursive("1", "(n+1)::bigint", "n<2"),
            recursive("1", "(n+1)::bigint"),
            recursive("1::smallint", "n+1"),
            recursive("'a'::varchar(1)", "(n||'b')::varchar(2)", "n='a'"),
            recursive("'a'::varchar(1)", "n::varchar"),
            recursive("'a'::varchar(70000)", "n::varchar(70001)"),
            recursive("'a'::varchar(1)", "CASE WHEN true THEN n ELSE 'b' END"),
            recursive("1::numeric(3,1)", "n::numeric(4,1)"),
            recursive("1::numeric(3,1)", "n+1"),
            recursive("'a'::char(1)", "n::char(2)"),
            recursive("'1'::bit(1)", "n::bit(2)"),
            recursive("'12:00:00'::time(1)", "n::time(2)"),
            "WITH RECURSIVE q(n) AS (SELECT v FROM recursive_type_source UNION ALL SELECT n::varchar(2) FROM q WHERE false) SELECT n FROM q",
            recursive("nextval('recursive_type_calls')::integer", "n::bigint"),
        ]
        for sql in invalid:
            query(sql, state="42804")
            parse_reject(sql)
        parse_reject(recursive("$1", "n::bigint"), [23])
        query("SELECT currval('recursive_type_calls')", state="55000")
        valid = [
            (recursive("1", "n+1", "n<3"), [["1"], ["2"], ["3"]], [23]),
            (recursive("1::bigint", "(n+1)::integer", "n<2"), [["1"], ["2"]], [20]),
            (recursive("'a'::varchar", "(n||'b')::varchar(2)", "n='a'"), [["a"], ["ab"]], [1043]),
            (recursive("'a'::varchar(1)", "n::varchar(1)"), [["a"]], [1043]),
            (recursive("'a'::varchar(1)", "CASE WHEN true THEN n ELSE 'b'::varchar(1) END"), [["a"]], [1043]),
            (recursive("'a'::varchar(1)", "coalesce(n,'b'::varchar(1))"), [["a"]], [1043]),
            (recursive("1::numeric(3,1)", "n::numeric(3,1)"), [["1.0"]], [1700]),
            (recursive("NULL", "'x'::text", "n IS NULL"), [[None], ["x"]], [25]),
            ("WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT 2::bigint) SELECT n FROM q", [["1"], ["2"]], [20]),
            ("WITH RECURSIVE q(n) AS (SELECT 'a'::varchar(1) UNION ALL SELECT 'ab'::varchar(2)) SELECT n FROM q", [["a"], ["ab"]], [1043]),
        ]
        for sql, rows, oids in valid:
            query(sql, rows, oids)
        parse_describe(recursive("$1", "n::integer"), [(20, -1)], [20])
        parse_describe(recursive("'a'::varchar(1)", "n::varchar(1)"), [(1043, 5)])
        parse_describe("WITH RECURSIVE q(n) AS (SELECT 'a'::varchar(1) UNION ALL SELECT 'ab'::varchar(2)) SELECT n FROM q", [(1043, -1)])
        parse_describe(recursive("1", "n::" + domain), [(23, -1)])
        # PostgreSQL's wire descriptor flattens domains; internal preservation
        # of identical domains is checked by the metadata-only native driver.
        parse_describe(recursive("1::" + domain, "n"), [(23, -1)])
        parse_reject(recursive("1::" + domain, "n::integer"))
        parse_describe(recursive("'12:00:00'::time(6)", "n::time(7)"), [(1083, 6)])
        parse_reject(recursive("'12:00:00'::time(5)", "n::time(7)"))
        parse_reject(recursive("'a'::text", "'a'::" + enum_type))
        parse_describe(recursive("'a'::" + enum_type, "n"), [(enum_oid, -1)])
        assert controls == 57, controls
        assert not failures, failures
        print("[RECURSIVE CTE TYPE DECLARATION] 32 rejection, 10 success, 7 descriptor and no-effect controls passed")
    finally:
        try:
            c.simple_query(sock, "DROP SCHEMA IF EXISTS " + schema + " CASCADE")
        finally:
            if server:
                r.stop_ours(server)
            else:
                sock.close()


if __name__ == "__main__":
    main()
