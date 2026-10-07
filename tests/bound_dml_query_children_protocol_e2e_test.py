#!/usr/bin/env python3
"""Bound DML owns real SQL child cursors, planned roots and typed OLD rows."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("bound_children_runner", root / "tests/compat/pg_diff_runner.py")
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

    def query(sql, state=None, rows=None, tag=None, types=None):
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        print("BOUND CHILDREN", sql, result, flush=True)
        if result[1] != state: failures.append((sql, "state", result[1], state))
        if rows is not None and result[0] != rows: failures.append((sql, "rows", result[0], rows))
        if tag is not None and result[4] != tag: failures.append((sql, "tag", result[4], tag))
        if types is not None and result[5] != types: failures.append((sql, "types", result[5], types))
        if state and (result[0] or result[4] is not None): failures.append((sql, "partial output", result[0], result[4]))
        return result

    hint = "WITH hint AS(SELECT 1) "
    cases = [
        ("UPDATE bd_target SET v=v+1 WHERE id=ANY(SELECT id FROM bd_source WHERE id IS NOT NULL) RETURNING id,v", None, [["2", "21"], ["3", "31"]], "UPDATE 2", [23, 23], 0),
        ("DELETE FROM bd_target WHERE id=ANY(SELECT 2) RETURNING id,t", None, [["2", "bob"]], "DELETE 1", [23, 25], 0),
        (hint + "UPDATE bd_target SET v=v+1 WHERE id=ANY(SELECT id FROM bd_source WHERE id IS NOT NULL) RETURNING id,v", None, [["2", "21"], ["3", "31"]], "UPDATE 2", [23, 23], 0),
        (hint + "DELETE FROM bd_target WHERE id=ANY(SELECT id FROM bd_source WHERE false) RETURNING id", None, [], "DELETE 0", [23], 0),
        (hint + "UPDATE bd_target SET v=9 WHERE id=ALL(SELECT id FROM bd_source WHERE false) RETURNING id,v", None, [["1", "9"], ["2", "9"], ["3", "9"], ["4", "9"]], "UPDATE 4", [23, 23], 0),
        (hint + 'UPDATE bd_target AS "d.x" SET v="d.x".v+1 WHERE "d.x".id=ANY(SELECT r.id FROM bd_source AS r WHERE r.id="d.x".id) RETURNING "d.x".id,v', None, [["2", "21"], ["3", "31"]], "UPDATE 2", [23, 23], 0),
        (hint + "DELETE FROM bd_target AS d WHERE d.id=ANY(SELECT d.id) RETURNING id", None, [["1"], ["2"], ["3"], ["4"]], "DELETE 4", [23], 0),
        (hint + "UPDATE bd_target AS d SET v=(SELECT d.v+1) WHERE d.id=ANY(SELECT 2) RETURNING id,v", None, [["2", "21"]], "UPDATE 1", [23, 23], 0),
        (hint + "UPDATE bd_target AS d SET v=1 WHERE id=2 RETURNING id,id=ANY(SELECT id FROM bd_source),(SELECT d.v)", None, [["2", "t", "1"]], "UPDATE 1", [23, 16, 23], 0),
        (hint + "DELETE FROM bd_target AS d WHERE id=1 RETURNING id,id=ANY(SELECT NULL::INT),(SELECT d.t)", None, [["1", None, "ann"]], "DELETE 1", [23, 16, 25], 0),
        (hint + "DELETE FROM bd_target WHERE id<>ALL(SELECT id FROM bd_source) RETURNING id", None, [], "DELETE 0", [23], 0),
        (hint + "UPDATE bd_target SET v=1 WHERE id=ANY(SELECT bd_writer(id) FROM bd_source WHERE id IS NOT NULL) RETURNING id", None, [["2"], ["3"]], "UPDATE 2", [23], 2),
        (hint + "DELETE FROM bd_target AS d WHERE d.id=ANY(SELECT bd_writer(r.id) FROM bd_source AS r WHERE r.id=d.id) RETURNING id", None, [["2"], ["3"]], "DELETE 2", [23], 2),
        (hint + "UPDATE bd_target SET v=bd_writer(v) WHERE id=ANY(SELECT 2 UNION ALL SELECT 3) RETURNING id,v", None, [["2", "20"], ["3", "30"]], "UPDATE 2", [23, 23], 2),
        (hint + "DELETE FROM bd_target WHERE false AND id=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END)", "22012", [], None, None, 0),
        (hint + "DELETE FROM bd_target WHERE false AND 1=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END)", None, [], "DELETE 0", None, 0),
        (hint + "DELETE FROM bd_target WHERE CASE WHEN false THEN id=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END) ELSE false END", None, [], "DELETE 0", None, 0),
        (hint + "UPDATE bd_target SET v=1/0 WHERE false", "22012", [], None, None, 0),
        (hint + "UPDATE bd_target SET v=(SELECT 1/0) WHERE false", "22012", [], None, None, 0),
        (hint + "UPDATE bd_target SET v=CASE WHEN false THEN(SELECT 1/0) ELSE v END WHERE id=ANY(SELECT 2) RETURNING id,v", None, [["2", "20"]], "UPDATE 1", [23, 23], 0),
        (hint + "DELETE FROM bd_target WHERE id=ANY(SELECT CAST('bad' AS INT))", "22P02", [], None, None, 0),
        (hint + "DELETE FROM bd_target WHERE false AND id=ANY(SELECT missing_bd_column)", "42703", [], None, None, 0),
        (hint + "UPDATE bd_target SET v=1 WHERE false AND id=ANY(SELECT missing_bd_function(1))", "42883", [], None, None, 0),
        (hint + "UPDATE bd_target SET v=1/0,v=2 WHERE id=ANY(SELECT bd_writer(id) FROM bd_source)", "42601", [], None, None, 0),
        (hint + "UPDATE bd_target AS d SET v=CAST(CASE WHEN d.id=2 THEN '21' ELSE 'bad' END AS INT) WHERE d.id=ANY(SELECT bd_writer(id) FROM bd_source WHERE id IS NOT NULL) RETURNING id,v", "22P02", [], None, None, 2),
        (hint + "UPDATE bd_target AS d SET v=(SELECT r.id FROM bd_source AS r WHERE r.id IS NOT NULL) WHERE id=ANY(SELECT 2) RETURNING id", "21000", [], None, None, 0),
        (hint + "DELETE FROM bd_target AS d WHERE d.id=ANY(SELECT 2) RETURNING id,(SELECT bd_writer(d.id)),(SELECT bd_writer(d.id))", None, [["2", "2", "2"]], "DELETE 1", [23, 23, 23], 2),
        ("WITH writer AS(INSERT INTO bd_sink VALUES(bd_writer(99)) RETURNING id) UPDATE bd_target SET v=1/0 WHERE false", "22012", [], None, None, 0),
        ("WITH writer AS(INSERT INTO bd_sink VALUES(bd_writer(99)) RETURNING id) DELETE FROM bd_target WHERE id=ANY(SELECT id FROM writer) RETURNING id", None, [], "DELETE 0", [23], 1),
        ("WITH writer AS(INSERT INTO bd_sink VALUES(bd_writer(99)) RETURNING id) UPDATE bd_target SET v=1 WHERE id=ANY(SELECT id FROM bd_sink) RETURNING id", None, [], "UPDATE 0", [23], 1),
        ("WITH bd_target AS(SELECT 99 AS id) DELETE FROM bd_target WHERE id=ANY(SELECT 2) RETURNING id", None, [["2"]], "DELETE 1", [23], 0),
    ]
    initial = [["1", "10", "ann"], ["2", "20", "bob"], ["3", "30", "cat"], ["4", "40", None]]
    try:
        for sql in ["BEGIN", "CREATE TEMP TABLE bd_target(id INT PRIMARY KEY,v INT,t TEXT)", "INSERT INTO bd_target VALUES(1,10,'ann'),(2,20,'bob'),(3,30,'cat'),(4,40,NULL)", "CREATE TEMP TABLE bd_source(id INT)", "INSERT INTO bd_source VALUES(2),(3),(NULL)", "CREATE TEMP TABLE bd_sink(id INT)", "CREATE TEMP SEQUENCE bd_calls", "CREATE FUNCTION bd_writer(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('bd_calls'); INSERT INTO bd_sink VALUES(p); RETURN p; END$$"]:
            assert query(sql + ";")[1] is None
        expected = 0
        for sql, state, rows, tag, types, calls in cases:
            query("SAVEPOINT bd_case;")
            query(sql + ";", state, rows, tag, types)
            query("ROLLBACK TO bd_case;")
            query("SELECT id,v,t FROM bd_target ORDER BY id;", rows=initial, types=[23, 23, 25])
            query("SELECT id FROM bd_sink;", rows=[])
            expected += calls + 1
            query("SELECT nextval('bd_calls');", rows=[[str(expected)]], types=[20])
        query("ROLLBACK;")
        print("BOUND CHILD FAILURES", failures, flush=True)
        assert not failures, failures
        print("[BOUND DML CHILDREN] passed whole matrix", flush=True)
    finally:
        try: c.simple_query(sock, "ROLLBACK;")
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == "__main__": main()
