"""pg_typeof uses real declarations and preserves demand; PostgreSQL 18 oracle."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("typeof_runner", root / "tests/compat/pg_diff_runner.py")
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

    def check(sql, rows=None, oids=None, state=None, describe=False):
        nonlocal checked
        checked += 1
        if describe:
            parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        actual = runner.decode_wire_result(messages, include_types=True)
        good = actual[1] == state
        if rows is not None:
            good = good and actual[0] == rows
        if oids is not None:
            good = good and actual[5] == oids
        if state:
            good = good and actual[0] == [] and actual[4] is None
        print("PG_TYPEOF", sql, "describe=", describe, actual, "pass=", good, flush=True)
        if not good:
            failures.append((sql, describe, actual, rows, oids, state))

    def parameter(oid, raw, expected):
        nonlocal checked
        checked += 1
        parse = b"\0SELECT pg_typeof($1)\0" + struct.pack("!HI", 1, oid)
        datum = struct.pack("!i", -1) if raw is None else struct.pack("!i", len(raw)) + raw
        bind = b"\0\0" + struct.pack("!HH", 0, 1) + datum + struct.pack("!H", 0)
        sock.sendall(client.typed(b"P", parse) + client.typed(b"D", b"S\0") +
                     client.typed(b"B", bind) + client.typed(b"D", b"P\0") +
                     client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
        messages = client.read_until_ready(sock)
        actual = runner.decode_wire_result(messages, include_types=True)
        descriptions = [client.row_description_fields([message]) for message in messages if message[0] == b"T"]
        good = actual[1] is None and actual[0] == [[expected]] and len(descriptions) == 2
        good = good and all(len(fields) == 1 and fields[0][3] == 2206 for fields in descriptions)
        print("PG_TYPEOF_PARAMETER", oid, raw, actual, "pass=", good, flush=True)
        if not good:
            failures.append((oid, raw, expected, actual, descriptions))

    try:
        cases = [
            ("SELECT 1,pg_typeof(1),pg_typeof(2147483648),pg_typeof(1.5)", [["1", "integer", "bigint", "numeric"]], [23, 2206, 2206, 2206]),
            ("SELECT pg_typeof(NULL),pg_typeof('abc'),pg_typeof(NULL::INT),pg_typeof(NULL::NAME)", [["unknown", "unknown", "integer", "name"]], [2206] * 4),
            ("SELECT pg_catalog.pg_typeof(true),pg_typeof(CURRENT_DATE),pg_typeof(CURRENT_USER)", [["boolean", "date", "name"]], [2206] * 3),
            ("SELECT pg_typeof(ARRAY[1,2]),pg_typeof(NULL::INTEGER[]),pg_typeof('integer'::REGTYPE)", [["integer[]", "integer[]", "regtype"]], [2206] * 3),
            ("SELECT pg_typeof(1)::OID,pg_typeof(NULL)::OID,pg_typeof(NULL::SMALLINT)::OID", [["23", "705", "21"]], [26] * 3),
            ("SELECT pg_typeof(CASE WHEN true THEN NULL::SMALLINT ELSE 1::SMALLINT END)", [["smallint"]], [2206]),
            ("SELECT pg_typeof(pg_typeof(NULL)),pg_typeof(NULL::VARCHAR(7))", [["regtype", "character varying"]], [2206, 2206]),
            ("SELECT pg_typeof(1) WHERE false", [], [2206]),
        ]
        for sql, rows, oids in cases:
            check(sql, rows, oids)
            check(sql, [], oids, describe=True)
        for oid, raw, expected in [(21, None, "smallint"), (21, b"7", "smallint"),
                                   (1043, None, "character varying"), (1007, None, "integer[]")]:
            parameter(oid, raw, expected)
        for sql, state in [
            ("SELECT pg_typeof()", "42883"),
            ("SELECT pg_typeof(1,2) WHERE false", "42883"),
            ("SELECT pg_typeof(a => 1)", "42883"),
            ("SELECT pg_typeof(1/0)", "22012"),
            ("SELECT pg_typeof(1) OVER ()", "42809"),
        ]:
            check(sql, state=state)
        check("CREATE TABLE typeof_source(i SMALLINT,t TEXT)", [])
        check("INSERT INTO typeof_source VALUES(NULL,NULL),(1,'abc')", [])
        check("SELECT pg_typeof(i),pg_typeof(t) FROM typeof_source", [["smallint", "text"], ["smallint", "text"]], [2206, 2206])
        check("SELECT pg_typeof(i),pg_typeof(t) FROM typeof_source WHERE false", [], [2206, 2206])
        check("CREATE TYPE typeof_rank AS ENUM('low','high')", [])
        check("CREATE DOMAIN typeof_score AS SMALLINT", [])
        check("SELECT pg_typeof(NULL::typeof_rank)::OID='typeof_rank'::REGTYPE::OID,pg_typeof(NULL::typeof_score)::OID='typeof_score'::REGTYPE::OID", [["t", "t"]], [16, 16])
        check("CREATE TABLE typeof_custom(r typeof_rank,s typeof_score)", [])
        check("INSERT INTO typeof_custom VALUES(NULL,NULL)", [])
        check("SELECT pg_typeof(r)::OID='typeof_rank'::REGTYPE::OID,pg_typeof(s)::OID='typeof_score'::REGTYPE::OID FROM typeof_custom", [["t", "t"]], [16, 16])
        check("DROP TABLE typeof_custom", [])
        check("DROP TYPE typeof_rank", [])
        check("DROP DOMAIN typeof_score", [])
        check("CREATE SEQUENCE typeof_effect", [])
        check("SELECT pg_typeof(nextval('typeof_effect'))", [], [2206], describe=True)
        check("SELECT pg_typeof(nextval('typeof_effect'))", [["bigint"]], [2206])
        check("SELECT currval('typeof_effect')", [["1"]], [20])
        check("SELECT pg_typeof(nextval('typeof_effect')) WHERE false", [], [2206])
        check("SELECT pg_typeof(nextval('typeof_effect')) LIMIT 0", [], [2206])
        check("SELECT currval('typeof_effect')", [["1"]], [20])
        check("CREATE FUNCTION public.pg_typeof(x INT) RETURNS INT LANGUAGE SQL AS $$SELECT 17$$", [])
        check("SELECT public.pg_typeof(1),pg_catalog.pg_typeof(1)", [["17", "integer"]], [23, 2206])
        check("SET search_path TO public,pg_catalog", [])
        check("SELECT pg_typeof(1)", [["17"]], [23])
        check("SET search_path TO public", [])
        check("SELECT pg_typeof(1),pg_catalog.pg_typeof(1)", [["17", "integer"]], [23, 2206])
        check("SELECT pg_typeof('abc'::TEXT)", [["text"]], [2206])
        check("SELECT pg_typeof(NULL)", state="42725")
        check("DROP FUNCTION public.pg_typeof", [])
        check("DROP SEQUENCE typeof_effect", [])
        check("DROP TABLE typeof_source", [])
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)
    print("PG_TYPEOF_CHECKED=", checked, "FAILED=", len(failures), flush=True)
    assert not failures, failures


if __name__ == "__main__":
    main()
