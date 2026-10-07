#!/usr/bin/env python3
"""Retained whole set-query Parse/Describe diagnostic; never execute targets."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("set_metadata_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        h, p, u, d, pw = r._reference_connection_settings()
        sock = socket.create_connection((h, p), timeout=r.wire_timeout())
        c.startup_reference(sock, u, d, pw); r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c); sock = server["sock"]
    failures = []

    def simple(sql):
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        print("SET META SQL", sql, result, flush=True)
        return result

    cases = [
        ("SELECT 1 UNION ALL SELECT 2147483648", [20], None),
        ("SELECT NULL UNION ALL SELECT 1", [23], None),
        ("SELECT 1 UNION ALL SELECT NULL", [23], None),
        ("SELECT NULL UNION ALL SELECT NULL", [25], None),
        ("SELECT '1' UNION ALL SELECT 2", [23], None),
        ("SELECT 1 UNION ALL SELECT '2'", [23], None),
        ("SELECT CAST(1 AS SMALLINT) UNION ALL SELECT 2", [23], None),
        ("SELECT CAST(1 AS REAL) UNION ALL SELECT 2", [700], None),
        ("SELECT ARRAY[1] UNION ALL SELECT ARRAY[2147483648]", [1016], None),
        ("SELECT id FROM source UNION ALL SELECT wide FROM source", [20], None),
        ("SELECT writer(1) UNION ALL SELECT 2147483648", [20], None),
        ("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 2147483648", [20], None),
        ("SELECT 1 UNION ALL SELECT 'bad'", None, "22P02"),
        ("SELECT 'bad' UNION ALL SELECT 1", None, "22P02"),
        ("SELECT 1 UNION ALL SELECT CAST('bad' AS TEXT)", None, "42804"),
        ("SELECT TRUE UNION ALL SELECT 1", None, "42804"),
        ("SELECT 1 UNION ALL SELECT 2,3", None, "42601"),
        ("SELECT 1 UNION ALL SELECT 'bad' WHERE missing_set_function(1)=1", None, "42883"),
    ]
    try:
        assert simple("BEGIN;")[1] is None
        if reference:
            schema = "set_meta_" + uuid.uuid4().hex
            assert simple("CREATE SCHEMA " + schema + ";")[1] is None
            assert simple("SET LOCAL search_path=" + schema + ",pg_temp,pg_catalog;")[1] is None
        for sql in ["CREATE TEMP TABLE source(id INT,wide BIGINT,t TEXT)", "CREATE TEMP SEQUENCE set_meta_calls", "CREATE FUNCTION writer(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN RETURN nextval('set_meta_calls')::INT; END$$"]:
            assert simple(sql + ";")[1] is None
        for index, (sql, expected, state) in enumerate(cases):
            assert simple("SAVEPOINT set_meta_case;")[1] is None
            name = ("set_meta_" + str(index)).encode()
            parse = name + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            sock.sendall(c.typed(b"P", parse) + c.typed(b"D", b"S" + name + b"\0") + c.typed(b"S"))
            messages = c.read_until_ready(sock)
            error = next((r.decode_wire_result([message], include_types=True)[1]
                          for message in messages if message[0] == b"E"), None)
            fields = c.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
            actual = [field[3] for field in fields]
            print("SET META DESCRIBE", sql, error, fields, flush=True)
            if error != state: failures.append((sql, "state", error, state))
            if expected is not None and actual != expected: failures.append((sql, "types", actual, expected))
            if expected is not None and any(field[1] or field[2] for field in fields):
                failures.append((sql, "set output must not inherit physical origin", fields))
            if any(kind in (b"D", b"C") for kind, _ in messages): failures.append((sql, "metadata executed a target"))
            if state is not None and any(kind == b"1" for kind, _ in messages):
                failures.append((sql, "failed analysis published a named statement"))
            assert simple("ROLLBACK TO set_meta_case;")[1] is None
            if state is not None:
                sock.sendall(c.typed(b"D", b"S" + name + b"\0") + c.typed(b"S"))
                missing = r.decode_wire_result(c.read_until_ready(sock), include_types=True)
                print("SET META FAILED NAME", sql, missing, flush=True)
                if missing[1] != "26000": failures.append((sql, "failed prepared name exists", missing))
                assert simple("ROLLBACK TO set_meta_case;")[1] is None
            else:
                portal = ("set_portal_" + str(index)).encode()
                bind = portal + b"\0" + name + b"\0" + struct.pack("!HHH", 0, 0, 0)
                sock.sendall(c.typed(b"B", bind) + c.typed(b"D", b"P" + portal + b"\0") + c.typed(b"S"))
                portal_messages = c.read_until_ready(sock)
                portal_result = r.decode_wire_result(portal_messages, include_types=True)
                portal_fields = c.row_description_fields(portal_messages) if any(kind == b"T" for kind, _ in portal_messages) else []
                print("SET META PORTAL", sql, portal_result, portal_fields, flush=True)
                if portal_result[1] is not None: failures.append((sql, "portal analysis", portal_result))
                if [field[3] for field in portal_fields] != expected: failures.append((sql, "portal types", portal_fields, expected))
                if any(field[1] or field[2] for field in portal_fields): failures.append((sql, "portal physical origin", portal_fields))
                if any(kind in (b"D", b"C") for kind, _ in portal_messages): failures.append((sql, "portal metadata executed a target"))
        assert simple("SAVEPOINT set_meta_noeffects;")[1] is None
        value = simple("SELECT currval('set_meta_calls');")
        if value[1] != "55000": failures.append(("metadata-only writer effect", value))
        simple("ROLLBACK TO set_meta_noeffects;")
        simple("ROLLBACK;")
        print("SET META FAILURES", failures, flush=True)
        assert not failures, failures
    finally:
        try: c.simple_query(sock, "ROLLBACK;")
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == "__main__": main()
