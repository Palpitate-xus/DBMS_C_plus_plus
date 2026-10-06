#!/usr/bin/env python3
"""Unsplit ordinary-input consumer diagnostic; known failures stay asserted.

The metadata-only foundation does not close legacy UPDATE error propagation or
INSERT SELECT unknown-WHERE conversion. This script deliberately remains red
on that foundation rather than relaxing either PostgreSQL18 expectation.
"""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    directory = Path(__file__).resolve().parent
    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    runner = load("assignment_runner", directory / "compat/pg_diff_runner.py")
    controls = load("assignment_controls", directory / "compat/prepared_primitive_assignment_reference18.py")
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []
    def query(sql, state=None, rows=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("ASSIGNMENT WIRE", sql, result, flush=True)
        if result[1] != state:
            failures.append((sql, "state", result[1], state))
        if rows is not None and result[0] != rows:
            failures.append((sql, "rows", result[0], rows))
        if state and (result[0] or result[4] is not None):
            failures.append((sql, "partial publication", result))
        return result
    try:
        for sql in ("BEGIN", "CREATE TEMP TABLE assignment_rows(id INT)",
                    "INSERT INTO assignment_rows VALUES(1)", "CREATE TEMP SEQUENCE assignment_effects",
                    "CREATE FUNCTION assignment_writer(p INT) RETURNS INT LANGUAGE plpgsql AS "
                    "$$BEGIN PERFORM nextval('assignment_effects'); RETURN p; END$$"):
            query(sql + ";")
            assert not failures, failures
        for index, (sql, state) in enumerate(controls.CASES, 1):
            query("SAVEPOINT assignment_case;")
            query(sql + ";", state)
            query("ROLLBACK TO assignment_case;")
            query("SELECT id FROM assignment_rows;", rows=[["1"]])
            sentinel = query("SELECT nextval('assignment_effects');", rows=[[str(index)]])
            if sentinel[5] != [20]:
                failures.append((sql, "sequence OID", sentinel[5]))
        query("UPDATE assignment_rows SET id='2' WHERE 'true' RETURNING id;", rows=[["2"]])
        query("UPDATE assignment_rows SET id=NULL WHERE 'true' RETURNING id;", rows=[[None]])
        query("INSERT INTO assignment_rows SELECT '3' WHERE 'true' RETURNING id;", rows=[["3"]])
        query("SELECT id FROM assignment_rows ORDER BY id NULLS LAST;", rows=[["3"], [None]])
        query("ROLLBACK;")
        print("ASSIGNMENT WIRE FAILURES", failures, flush=True)
        assert not failures, failures
    finally:
        try:
            client.simple_query(sock, "ROLLBACK;")
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == "__main__":
    main()
