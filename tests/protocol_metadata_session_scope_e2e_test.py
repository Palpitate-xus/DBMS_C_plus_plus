#!/usr/bin/env python3
"""Fresh Parse/Describe owns search_path/TEMP without priming an error first."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "metadata_session_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    failures = []
    controls = 0
    cases = [
        ("integer_range", "SELECT sum(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS total,avg(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS mean FROM inputs", ["total", "mean"], [20, 1700]),
        ("stored_result", "SELECT sum(CASE WHEN i>0 THEN writer(i) ELSE 0 END) AS total FROM inputs", ["total"], [20]),
        ("temp_child", "SELECT (SELECT id FROM source LIMIT 1) AS value", ["value"], [23]),
        ("temp_set", "SELECT id FROM source UNION ALL SELECT wide FROM source", ["id"], [20]),
    ]
    for label, sql, headers, types in cases:
        # Each case must be the first extended query of its own connection.
        # The old error cleanup accidentally primed later metadata lookups.
        server = None
        if reference:
            host, port, user, database, password = runner._reference_connection_settings()
            sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
            client.startup_reference(sock, user, database, password)
            runner.verify_reference_version(client, sock)
        else:
            server = runner.start_ours(client)
            sock = server["sock"]

        def query(statement):
            result = runner.decode_wire_result(client.simple_query(sock, statement), include_types=True)
            assert result[1] is None, (label, statement, result)
            return result

        def describe(phase):
            nonlocal controls
            controls += 1
            name = ("metadata_" + uuid.uuid4().hex[:12]).encode()
            sock.sendall(client.typed(b"P", name + b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"D", b"S" + name + b"\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
            result = runner.decode_wire_result(messages, include_types=True)
            print("METADATA_SESSION", label, phase, result, flush=True)
            valid = (result[1] is None and result[3] == headers and result[5] == types and
                     sum(kind == b"1" for kind, _ in messages) == 1 and
                     not any(kind in (b"D", b"C") for kind, _ in messages))
            if not valid:
                failures.append((label, phase, result, messages))
                return
            # Bind planning and portal Describe also remain metadata-only.
            portal = name + b"p"
            sock.sendall(client.typed(b"B", portal + b"\0" + name + b"\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P" + portal + b"\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
            result = runner.decode_wire_result(messages, include_types=True)
            if (result[1] is not None or result[3] != headers or result[5] != types or
                    any(kind in (b"D", b"C") for kind, _ in messages)):
                failures.append((label, phase, "portal", result, messages))
            sock.sendall(client.typed(b"C", b"P" + portal + b"\0") +
                         client.typed(b"C", b"S" + name + b"\0") + client.typed(b"S"))
            assert not any(kind == b"E" for kind, _ in client.read_until_ready(sock))

        try:
            if reference:
                query("BEGIN")
            schema = "metadata_session_" + uuid.uuid4().hex[:12]
            query("CREATE SCHEMA " + schema)
            query("SET search_path TO " + schema + ",pg_catalog")
            query("CREATE TABLE inputs(i INTEGER)")
            query("INSERT INTO inputs VALUES(1),(2),(NULL)")
            query("CREATE TEMP TABLE source(id INTEGER,wide BIGINT)")
            query("CREATE SEQUENCE effects")
            query("CREATE FUNCTION writer(i INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('effects'); RETURN i; END;$$")
            query("SELECT nextval('effects')")
            describe("first")
            describe("repeat")
            if reference:
                query("SAVEPOINT metadata_error")
            bad = b"metadata_bad"
            sock.sendall(client.typed(b"P", bad + b"\0SELECT 1 UNION ALL SELECT 'not-an-integer'\0\0\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
            result = runner.decode_wire_result(messages, include_types=True)
            assert result[1] == "22P02", (label, "error boundary", result)
            assert not any(kind == b"1" for kind, _ in messages), messages
            if reference:
                query("ROLLBACK TO metadata_error")
                query("RELEASE metadata_error")
            describe("after_error")
            assert query("SELECT currval('effects')")[0] == [["1"]], label
        finally:
            if server:
                runner.stop_ours(server)
            else:
                try:
                    client.simple_query(sock, "ROLLBACK")
                finally:
                    sock.close()
    assert controls == 12, controls
    assert not failures, ("metadata session scope failures", failures)
    print("[PROTOCOL METADATA SESSION] all 12 fresh/repeat/error controls passed; no writer execution")


if __name__ == "__main__":
    main()
