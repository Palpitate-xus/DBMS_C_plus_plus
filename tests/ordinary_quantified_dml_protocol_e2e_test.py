#!/usr/bin/env python3
"""Ordinary I/U/D executes genuine typed SQL/array quantifiers atomically."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("ordinary_q_runner", root / "tests/compat/pg_diff_runner.py")
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
        print("ORDINARY Q DML", sql, result, flush=True)
        if result[1] != state: failures.append((sql, "state", result[1], state))
        if rows is not None and result[0] != rows: failures.append((sql, "rows", result[0], rows))
        if tag is not None and result[4] != tag: failures.append((sql, "tag", result[4], tag))
        if types is not None and result[5] != types: failures.append((sql, "types", result[5], types))
        if state and (result[0] or result[4] is not None): failures.append((sql, "partial", result[0], result[4]))
        return result

    cases = [
        ("UPDATE oq_target SET v=v+1 WHERE id=ANY(SELECT id FROM oq_source WHERE id IS NOT NULL) RETURNING id,v", None, [["2", "21"], ["3", "31"]], "UPDATE 2", [23, 23], 0),
        ('UPDATE oq_target AS "d.x" SET v=(SELECT "d.x".v+1) WHERE "d.x".id=ANY(SELECT r.id FROM oq_source AS r WHERE r.id="d.x".id) RETURNING "d.x".id,v', None, [["2", "21"], ["3", "31"]], "UPDATE 2", [23, 23], 0),
        ("DELETE FROM oq_target WHERE id=ALL(SELECT id FROM oq_source WHERE false) RETURNING id", None, [["1"], ["2"], ["3"], ["4"]], "DELETE 4", [23], 0),
        ("DELETE FROM oq_target WHERE id<>ALL(SELECT id FROM oq_source) RETURNING id", None, [], "DELETE 0", [23], 0),
        ("UPDATE oq_target SET v=9 WHERE id=ANY(ARRAY[2,3,NULL]) RETURNING id", None, [["2"], ["3"]], "UPDATE 2", [23], 0),
        ("DELETE FROM oq_target WHERE id=ANY(SELECT NULL::INT) RETURNING id", None, [], "DELETE 0", [23], 0),
        ("INSERT INTO oq_output VALUES(5,2=ANY(SELECT id FROM oq_source WHERE id IS NOT NULL)) RETURNING id,flag,1=ALL(SELECT id FROM oq_source WHERE false)", None, [["5", "t", "t"]], "INSERT 0 1", [23, 16, 16], 0),
        ("INSERT INTO oq_output VALUES(6,2=ANY(SELECT NULL::INT)) RETURNING id,flag", None, [["6", None]], "INSERT 0 1", [23, 16], 0),
        ("DELETE FROM oq_target WHERE id=ANY(SELECT 2) RETURNING id,t,id=ANY(SELECT 2),(SELECT t)", None, [["2", "", "t", ""]], "DELETE 1", [23, 25, 16, 25], 0),
        ("UPDATE oq_target SET v=1 WHERE id=ANY(SELECT oq_writer(id) FROM oq_source WHERE id IS NOT NULL) RETURNING id", None, [["2"], ["3"]], "UPDATE 2", [23], 2),
        ("DELETE FROM oq_target AS d WHERE d.id=ANY(SELECT oq_writer(r.id) FROM oq_source AS r WHERE r.id=d.id) RETURNING id", None, [["2"], ["3"]], "DELETE 2", [23], 2),
        ("DELETE FROM oq_target AS d WHERE d.id=ANY(SELECT 2) RETURNING id,(SELECT oq_writer(d.id)),(SELECT oq_writer(d.id))", None, [["2", "2", "2"]], "DELETE 1", [23, 23, 23], 2),
        ("UPDATE oq_target SET v=1/0 WHERE false AND 1=ANY(SELECT oq_writer(1))", "22012", [], None, None, 0),
        ("UPDATE oq_target SET v=(SELECT 1/0) WHERE false AND 1=ANY(SELECT oq_writer(1))", "22012", [], None, None, 0),
        ("DELETE FROM oq_target WHERE false AND 1=ANY(SELECT 1/0)", None, [], "DELETE 0", None, 0),
        ("UPDATE oq_target SET v=CASE WHEN false THEN(SELECT 1/0) ELSE v END WHERE id=ANY(SELECT 2) RETURNING id,v", None, [["2", "20"]], "UPDATE 1", [23, 23], 0),
        ("UPDATE oq_target SET v=1/0,v=2 WHERE id=ANY(SELECT oq_writer(id) FROM oq_source)", "42601", [], None, None, 0),
        ("DELETE FROM oq_target WHERE false AND id=ANY(SELECT missing_oq_column)", "42703", [], None, None, 0),
        ("UPDATE oq_target SET v=1 WHERE false AND id=ANY(SELECT missing_oq_function(1))", "42883", [], None, None, 0),
        ("DELETE FROM oq_target WHERE id=ANY(SELECT CAST('bad' AS INT))", "22P02", [], None, None, 0),
        ("UPDATE oq_target SET v=(SELECT id FROM oq_source WHERE id IS NOT NULL) WHERE id=ANY(SELECT 2) RETURNING id", "21000", [], None, None, 0),
        ("UPDATE oq_target AS d SET v=CAST(CASE WHEN d.id=2 THEN '21' ELSE 'bad' END AS INT) WHERE d.id=ANY(SELECT oq_writer(id) FROM oq_source WHERE id IS NOT NULL) RETURNING id,v", "22P02", [], None, None, 2),
        ("INSERT INTO oq_output VALUES(7,1=ANY(SELECT missing_oq_function(1)))", "42883", [], None, None, 0),
    ]
    initial = [["1", "10", None], ["2", "20", ""], ["3", "30", "NULL"], ["4", "40", "spaces ' quotes"]]
    try:
        for sql in ["BEGIN", "CREATE TEMP TABLE oq_target(id INT PRIMARY KEY,v INT,t TEXT)", "INSERT INTO oq_target VALUES(1,10,NULL),(2,20,''),(3,30,'NULL'),(4,40,'spaces '' quotes')", "CREATE TEMP TABLE oq_source(id INT)", "INSERT INTO oq_source VALUES(2),(3),(NULL)", "CREATE TEMP TABLE oq_output(id INT,flag BOOL)", "CREATE TEMP TABLE oq_sink(id INT)", "CREATE TEMP SEQUENCE oq_calls", "CREATE FUNCTION oq_writer(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('oq_calls'); INSERT INTO oq_sink VALUES(p); RETURN p; END$$"]:
            assert query(sql + ";")[1] is None
        expected = 0
        for sql, state, rows, tag, types, calls in cases:
            query("SAVEPOINT oq_case;")
            query(sql + ";", state, rows, tag, types)
            query("ROLLBACK TO oq_case;")
            query("SELECT id,v,t FROM oq_target ORDER BY id;", rows=initial, types=[23, 23, 25])
            query("SELECT id FROM oq_sink;", rows=[])
            query("SELECT id,flag FROM oq_output;", rows=[], types=[23, 16])
            expected += calls + 1
            query("SELECT nextval('oq_calls');", rows=[[str(expected)]], types=[20])
        query("ROLLBACK;")
        print("ORDINARY Q FAILURES", failures, flush=True)
        assert not failures, failures
        print("[ORDINARY QUANTIFIED DML] passed whole 23-case matrix", flush=True)
    finally:
        try: c.simple_query(sock, "ROLLBACK;")
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == "__main__": main()
