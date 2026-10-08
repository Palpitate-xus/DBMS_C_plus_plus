#!/usr/bin/env python3
"""Execution errors retain their declared SQLSTATE across the wire boundary."""

import argparse
import importlib.util
from pathlib import Path
import socket
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference18", action="store_true")
    parser.add_argument("--collect-errors", action="store_true")
    options = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("errors_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = None
    owned_schema = None
    if options.reference18:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    failures = []
    controls = 0

    def query(sql):
        return runner.ours_query(client, sock, sql)

    def check(condition, detail):
        nonlocal controls
        controls += 1
        if not condition:
            failures.append(detail)
            if not options.collect_errors:
                raise AssertionError(detail)

    try:
        if options.reference18:
            owned_schema = "owned_subquery_state_" + uuid.uuid4().hex[:12]
            rows, state, message, _ = query("CREATE SCHEMA " + owned_schema)
            assert state is None, (state, message)
            rows, state, message, _ = query("SET search_path TO " + owned_schema + ",pg_catalog")
            assert state is None, (state, message)
        for sql in ("CREATE TABLE state_outer (id INT);",
                    "CREATE TABLE state_inner (id INT);",
                    "INSERT INTO state_outer VALUES (1);",
                    "INSERT INTO state_inner VALUES (1), (2);"):
            _, state, message, _ = query(sql)
            assert state is None, (sql, state, message)
        cases = [
            ("SELECT id FROM state_inner FETCH FIRST 1 ROW WITH TIES", "42601"),
            ("SELECT id FROM state_inner ORDER BY 2 LIMIT 1", "42P10"),
            ("SELECT id, id FROM state_inner LIMIT 1", "42601"),
            ("SELECT id FROM state_inner", "21000"),
            ("SELECT id FROM state_missing LIMIT 1", "42P01"),
            ("SELECT DISTINCT id FROM state_inner", "21000"),
        ]
        for inner, expected in cases:
            sql = "SELECT (" + inner + ") AS value FROM state_outer;"
            rows, state, message, _ = query(sql)
            check(state == expected, (sql, state, expected, message))
            check(rows == [], (sql, rows))
            check("SQLSTATE" not in message, (sql, message))
            # An autocommit error must not poison the connection or leave a lock.
            rows, state, message, _ = query("SELECT id FROM state_outer;")
            check(state is None and rows == [["1"]], (state, message, rows))

        # DISTINCT removes duplicates before scalar cardinality is checked.
        # One distinct non-NULL/NULL datum is valid; two distinct rows remain
        # a real 21000 error. Keep the original two-value statement unchanged.
        for sql in ("INSERT INTO state_inner VALUES (1), (NULL), (NULL);",):
            _, state, message, _ = query(sql)
            assert state is None, (sql, state, message)
        positive = [
            ("SELECT (SELECT DISTINCT id FROM state_inner WHERE id=1) AS value FROM state_outer;", [["1"]]),
            ("SELECT (SELECT DISTINCT id FROM state_inner WHERE id IS NULL) AS value FROM state_outer;", [[None]]),
            ("SELECT (SELECT DISTINCT id FROM state_inner WHERE id<0) AS value FROM state_outer;", [[None]]),
            ("SELECT (SELECT DISTINCT id FROM state_inner ORDER BY id LIMIT 1) AS value FROM state_outer;", [["1"]]),
            ("SELECT (SELECT DISTINCT id FROM state_inner) AS value FROM state_outer WHERE false;", []),
            ("SELECT (SELECT DISTINCT id FROM state_inner) AS value FROM state_outer LIMIT 0;", []),
        ]
        for sql, expected in positive:
            messages = client.simple_query(sock, sql)
            actual = runner.decode_wire_result(messages, include_types=True)
            check(actual[1] is None and actual[0] == expected, (sql, actual, expected))
            check(actual[3:] == (["value"], "SELECT " + str(len(expected)), [23]),
                  ("scalar declared type/header/tag", sql, actual))
        rows, state, message, _ = query(
            "SELECT (SELECT DISTINCT id FROM state_inner) AS value FROM state_outer;")
        check(state == "21000" and rows == [], ("distinct true cardinality", state, rows, message))
        rows, state, message, _ = query("SELECT id FROM state_outer;")
        check(state is None and rows == [["1"]], ("connection after final scalar error", state, rows, message))
        print("[SUBQUERY SQLSTATE E2E] complete", controls, "failures", len(failures), failures, flush=True)
        assert not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                if owned_schema:
                    query("SET search_path TO pg_catalog")
                    _, state, message, _ = query("DROP SCHEMA " + owned_schema + " CASCADE")
                    assert state is None, (state, message)
            finally:
                sock.close()


if __name__ == "__main__":
    main()
