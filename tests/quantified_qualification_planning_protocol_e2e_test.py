#!/usr/bin/env python3
"""Only eligible top-level local-Var ANY quals plan before boolean pruning."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("q_qual_runner", root / "tests/compat/pg_diff_runner.py")
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

    def query(sql, state=None, rows=None, tag=None):
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        print("Q QUAL PLANNING", sql, result, flush=True)
        if result[1] != state: failures.append((sql, "state", result[1], state))
        if rows is not None and result[0] != rows: failures.append((sql, "rows", result[0], rows))
        if tag is not None and result[4] != tag: failures.append((sql, "tag", result[4], tag))
        if state and (result[0] or result[4] is not None): failures.append((sql, "partial", result[0], result[4]))
        return result

    cases = [
        ("DELETE FROM qq_rows WHERE false AND id=ANY(SELECT 1/0)", "22012", [], None),
        ("DELETE FROM qq_rows WHERE false AND 1=ANY(SELECT 1/0)", None, [], "DELETE 0"),
        ("DELETE FROM qq_rows WHERE false AND id=ALL(SELECT 1/0)", None, [], "DELETE 0"),
        ("DELETE FROM qq_rows WHERE true OR id=ANY(SELECT 1/0) RETURNING id", None, [["1"], ["2"]], "DELETE 2"),
        ("DELETE FROM qq_rows WHERE false AND(id=ANY(SELECT 1/0) OR true)", None, [], "DELETE 0"),
        ("DELETE FROM qq_rows WHERE false AND CASE WHEN true THEN id=ANY(SELECT 1/0) ELSE false END", None, [], "DELETE 0"),
        ("DELETE FROM qq_rows WHERE false AND(id+0)=ANY(SELECT 1/0)", "22012", [], None),
        ("DELETE FROM qq_rows WHERE false AND abs(id)=ANY(SELECT 1/0)", "22012", [], None),
        ("DELETE FROM qq_rows WHERE false AND qq_writer(id)=ANY(SELECT 1/0)", None, [], "DELETE 0"),
        ("DELETE FROM qq_rows WHERE false AND(SELECT id)=ANY(SELECT 1/0)", "22012", [], None),
        ("DELETE FROM qq_rows WHERE false AND id=ANY(SELECT 1/0 WHERE false)", "22012", [], None),
        ("DELETE FROM qq_rows WHERE false AND id=ANY(SELECT qq_writer(1/0))", "22012", [], None),
        ("SELECT id FROM qq_rows WHERE false AND id=ANY(SELECT 1/0)", "22012", [], None),
        ("SELECT CASE WHEN false THEN id=ANY(SELECT 1/0) ELSE false END FROM qq_rows", None, [["f"], ["f"]], "SELECT 2"),
        ("WITH hint AS(SELECT 1) DELETE FROM qq_rows WHERE false AND id=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END)", "22012", [], None),
        ("UPDATE qq_rows SET id=CAST(2147483648 AS INT) WHERE false AND id=ANY(SELECT 1/0)", "22003", [], None),
        ("DELETE FROM qq_rows WHERE false AND(SELECT qq_writer(id))=ANY(SELECT 1/0)", None, [], "DELETE 0"),
        ("DELETE FROM qq_rows WHERE false AND(SELECT q.id FROM qq_rows q LIMIT 1)=ANY(SELECT 1/0)", None, [], "DELETE 0"),
        ("DELETE FROM qq_rows WHERE false AND(SELECT q.id FROM qq_rows q WHERE q.id=qq_rows.id)=ANY(SELECT 1/0)", "22012", [], None),
    ]
    try:
        for sql in ["BEGIN", "CREATE TEMP TABLE qq_rows(id INT)", "INSERT INTO qq_rows VALUES(1),(2)", "CREATE TEMP SEQUENCE qq_calls", "CREATE FUNCTION qq_writer(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('qq_calls'); RETURN p; END$$"]:
            assert query(sql + ";")[1] is None
        for sql, state, rows, tag in cases:
            query("SAVEPOINT qq_case;")
            query(sql + ";", state, rows, tag)
            query("ROLLBACK TO qq_case;")
            query("SELECT id FROM qq_rows ORDER BY id;", rows=[["1"], ["2"]], tag="SELECT 2")
            query("SAVEPOINT qq_effect;")
            query("SELECT currval('qq_calls');", state="55000", rows=[])
            query("ROLLBACK TO qq_effect;")
        query("ROLLBACK;")
        print("Q QUAL FAILURES", failures, flush=True)
        assert not failures, failures
    finally:
        try: c.simple_query(sock, "ROLLBACK;")
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == "__main__": main()
