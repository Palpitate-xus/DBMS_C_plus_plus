#!/usr/bin/env python3
"""Set root and parenthesized branch clauses own actual typed row demand."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("set_clause_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r); c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        h, p, u, d, pw = r._reference_connection_settings()
        sock = socket.create_connection((h, p), timeout=r.wire_timeout())
        c.startup_reference(sock, u, d, pw); r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c); sock = server["sock"]
    failures = []; counter = 0

    def simple(sql, state=None, rows=None, types=None, labels=None):
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        print("SET CLAUSE", sql, result, flush=True)
        for key, actual, expected in [("state", result[1], state), ("rows", result[0], rows), ("types", result[5], types), ("labels", result[3], labels)]:
            if (key == "state" or expected is not None) and actual != expected:
                failures.append((sql, key, actual, expected))
        if state and (result[0] or result[4] is not None): failures.append((sql, "partial execution", result))
        if not state and rows is not None and result[4] != "SELECT " + str(len(rows)):
            failures.append((sql, "command tag", result[4]))
        return result

    cases = [
        ("SELECT 10 AS n UNION ALL SELECT 2 ORDER BY n LIMIT 1", None, [["2"]], [23], ["n"], 0),
        ("SELECT 10 AS n UNION ALL SELECT 2 ORDER BY 1 DESC OFFSET 1 LIMIT 1", None, [["2"]], [23], ["n"], 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 LIMIT 1", None, [["1"]], [23], ["n"], 0),
        ("SELECT 2 AS n UNION ALL SELECT 1 LIMIT 0", None, [], [23], ["n"], 0),
        ("SELECT 1 AS n UNION ALL (SELECT 2 LIMIT 0)", None, [["1"]], [23], ["n"], 0),
        ("(SELECT 3 AS n LIMIT 0) UNION ALL SELECT 2", None, [["2"]], [23], ["n"], 0),
        ("(SELECT 3 AS n ORDER BY n LIMIT 1) UNION ALL SELECT 2 ORDER BY n", None, [["2"], ["3"]], [23], ["n"], 0),
        ("SELECT 3 AS n UNION ALL (SELECT 2 ORDER BY 1 LIMIT 0) ORDER BY n", None, [["3"]], [23], ["n"], 0),
        ("SELECT 2 AS n UNION ALL SELECT 1 UNION ALL SELECT 3 ORDER BY n DESC OFFSET 1 FETCH NEXT 1 ROW ONLY", None, [["2"]], [23], ["n"], 0),
        ("SELECT NULL::INT AS n UNION ALL SELECT 2 ORDER BY n NULLS FIRST", None, [[None], ["2"]], [23], ["n"], 0),
        ("SELECT NULL::BIGINT AS n UNION ALL SELECT 2 ORDER BY n NULLS LAST", None, [["2"], [None]], [20], ["n"], 0),
        ("SELECT 1 AS n UNION ALL SELECT 2147483648 ORDER BY n DESC LIMIT 1", None, [["2147483648"]], [20], ["n"], 0),
        ("SELECT '' AS n UNION ALL SELECT 'NULL' UNION ALL SELECT NULL ORDER BY n NULLS FIRST", None, [[None], [""], ["NULL"]], [25], ["n"], 0),
        ("SELECT id AS n FROM clause_source UNION ALL SELECT 0 ORDER BY n LIMIT 1", None, [["0"]], [23], ["n"], 0),
        ("SELECT id FROM clause_source UNION ALL SELECT 0 ORDER BY id", None, [["0"], ["1"], ["2"]], [23], ["id"], 0),
        ("WITH c AS (SELECT 3 AS n) SELECT n FROM c UNION ALL SELECT n FROM c ORDER BY n LIMIT 1", None, [["3"]], [23], ["n"], 0),
        ("SELECT clause_writer(1) AS n UNION ALL SELECT clause_writer(2) LIMIT 1", None, [["1"]], [23], ["n"], 1),
        ("SELECT clause_writer(2) AS n UNION ALL SELECT clause_writer(1) ORDER BY n LIMIT 1", None, [["1"]], [23], ["n"], 2),
        ("(SELECT clause_writer(1) AS n LIMIT 0) UNION ALL SELECT clause_writer(2)", None, [["2"]], [23], ["n"], 1),
        ("SELECT clause_writer(1) AS n UNION ALL SELECT clause_writer(2) LIMIT 0", None, [], [23], ["n"], 0),
        ("SELECT clause_writer(1) AS n UNION ALL SELECT 1/0 LIMIT 0", "22012", [], None, None, 0),
        ("(SELECT clause_writer(1) AS n UNION ALL SELECT clause_writer(2) ORDER BY n LIMIT 1) UNION ALL SELECT clause_writer(3)", None, [["1"], ["3"]], [23], ["n"], 3),
        ("SELECT 1>ANY(SELECT clause_writer(0) AS n UNION ALL SELECT clause_writer(2) LIMIT 1)", None, [["t"]], [16], ["?column?"], 1),
        ("SELECT 1>ANY(SELECT clause_writer(0) AS n UNION ALL SELECT clause_writer(2) ORDER BY n LIMIT 1)", None, [["t"]], [16], ["?column?"], 2),
        ("SELECT (SELECT clause_writer(7) AS n UNION ALL SELECT clause_writer(8) LIMIT 1)", None, [["7"]], [23], ["n"], 1),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY missing", "42703", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 AS rhs ORDER BY rhs", "42703", [], None, None, 0),
        ("SELECT id AS n FROM clause_source UNION ALL SELECT 2 ORDER BY clause_source.id", "42P01", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n+1", "0A000", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY 0", "42P10", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY 2", "42P10", [], None, None, 0),
        ("SELECT 1 AS x,2 AS x UNION ALL SELECT 3,4 ORDER BY x", "42702", [], None, None, 0),
        ("SELECT 1 LIMIT 1 UNION ALL SELECT 2", "42601", [], None, None, 0),
        ("SELECT 1 ORDER BY 1 UNION ALL SELECT 2", "42601", [], None, None, 0),
        ("SELECT clause_writer(1) AS n UNION ALL SELECT 2 ORDER BY missing", "42703", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY missing_clause_function(n)", "42883", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY CAST('bad' AS INT)", "22P02", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY -1", "42P10", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 ORDER BY 'n'", "42601", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 2 FETCH FIRST 1 ROW WITH TIES", "42601", [], None, None, 0),
        ("SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n FETCH FIRST 1 ROW WITH TIES", None, [["1"], ["1"]], [23], ["n"], 0),
        ("SELECT NULL::INT AS n UNION ALL SELECT NULL::INT UNION ALL SELECT 2 ORDER BY n NULLS FIRST OFFSET 1 FETCH FIRST 1 ROW WITH TIES", None, [[None]], [23], ["n"], 0),
        ("SELECT clause_writer(1) AS n UNION ALL SELECT clause_writer(1) UNION ALL SELECT clause_writer(2) ORDER BY n FETCH FIRST 1 ROW WITH TIES", None, [["1"], ["1"]], [23], ["n"], 3),
    ]
    try:
        for sql in ["BEGIN", "CREATE TEMP TABLE clause_source(id INT)", "INSERT INTO clause_source VALUES(1),(2)", "CREATE TEMP SEQUENCE clause_calls", "CREATE FUNCTION clause_writer(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('clause_calls'); RETURN p; END$$"]:
            assert simple(sql + ";")[1] is None
        for index, (sql, state, rows, types, labels, effects) in enumerate(cases):
            assert simple("SAVEPOINT clause_case;")[1] is None
            name = ("clause_" + str(index)).encode()
            sock.sendall(c.typed(b"P", name + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)) + c.typed(b"D", b"S" + name + b"\0") + c.typed(b"S"))
            messages = c.read_until_ready(sock)
            metadata = r.decode_wire_result(messages, include_types=True)
            fields = c.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
            print("SET CLAUSE DESCRIBE", sql, metadata, fields, flush=True)
            # Runtime constant division belongs to planning, not Parse.
            metadata_state = None if state == "22012" else state
            if metadata[1] != metadata_state: failures.append((sql, "metadata state", metadata[1], metadata_state))
            if metadata_state is None and types is not None and [field[3] for field in fields] != types:
                failures.append((sql, "metadata types", fields, types))
            if metadata_state is None and types is not None and [field[0].decode() for field in fields] != labels:
                failures.append((sql, "metadata labels", fields, labels))
            if any(field[1] or field[2] for field in fields) and "UNION" in sql and sql.startswith(("SELECT", "WITH", "(")):
                failures.append((sql, "set physical origin", fields))
            if any(kind in (b"D", b"C") for kind, _ in messages): failures.append((sql, "metadata execution"))
            assert simple("ROLLBACK TO clause_case;")[1] is None
            simple(sql + ";", state, rows, types, labels)
            assert simple("ROLLBACK TO clause_case;")[1] is None
            counter += effects + 1
            simple("SELECT nextval('clause_calls');", rows=[[str(counter)]], types=[20])
        simple("ROLLBACK;")
        print("SET CLAUSE FAILURES", failures, flush=True)
        assert not failures, failures
    finally:
        try: c.simple_query(sock, "ROLLBACK;")
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == "__main__": main()
