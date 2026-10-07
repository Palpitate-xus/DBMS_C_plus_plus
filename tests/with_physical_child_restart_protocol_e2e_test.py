#!/usr/bin/env python3
"""Ancestor WITH frames must not pin a physical child's correlated row."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("with_restart_runner", root / "tests/compat/pg_diff_runner.py")
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
        print("WITH PHYSICAL RESTART", sql, result, flush=True)
        if result[1] != state: failures.append((sql, "state", result[1], state))
        if rows is not None and result[0] != rows: failures.append((sql, "rows", result[0], rows))
        if tag is not None and result[4] != tag: failures.append((sql, "tag", result[4], tag))
        if types is not None and result[5] != types: failures.append((sql, "types", result[5], types))
        if state and (result[0] or result[4] is not None): failures.append((sql, "partial", result[0], result[4]))
        return result

    hint = "WITH hint AS(SELECT 1) "
    cases = [
        (hint + 'UPDATE wr_target AS "d.x" SET v="d.x".v+1 WHERE "d.x".id=ANY(SELECT r.id FROM wr_source AS r WHERE r.id="d.x".id) RETURNING "d.x".id,v', None, [["2", "21"], ["3", "31"]], "UPDATE 2", [23, 23], 0),
        (hint + "DELETE FROM wr_target AS d WHERE d.id=ANY(SELECT wr_writer(r.id) FROM wr_source AS r WHERE r.id=d.id) RETURNING id,t", None, [["2", ""], ["3", "NULL"]], "DELETE 2", [23, 25], 2),
        (hint + "UPDATE wr_target AS d SET v=(SELECT r.id+10 FROM wr_source AS r WHERE r.id=d.id) WHERE d.id=ANY(SELECT id FROM wr_source WHERE id IS NOT NULL) RETURNING id,v", None, [["2", "12"], ["3", "13"]], "UPDATE 2", [23, 23], 0),
        (hint + "DELETE FROM wr_target AS d WHERE d.id=ANY(SELECT a.id FROM wr_source AS a JOIN wr_source AS b ON a.id=b.id WHERE a.id=d.id) RETURNING id", None, [["2"], ["3"]], "DELETE 2", [23], 0),
        ("WITH unused_writer AS(INSERT INTO wr_sink VALUES(wr_writer(99)) RETURNING id) DELETE FROM wr_target AS d WHERE d.id=ANY(SELECT wr_writer(r.id) FROM wr_source AS r WHERE r.id=d.id) RETURNING id", None, [["2"], ["3"]], "DELETE 2", [23], 3),
        (hint + "UPDATE wr_target AS d SET v=v WHERE d.id=ALL(SELECT r.id FROM wr_source AS r WHERE r.id=d.id) RETURNING id", None, [["1"], ["2"], ["3"], ["4"]], "UPDATE 4", [23], 0),
        (hint + "DELETE FROM wr_target AS d WHERE d.id=ANY(SELECT NULL::INT FROM wr_source AS r WHERE r.id=d.id) RETURNING id", None, [], "DELETE 0", [23], 0),
        (hint + "UPDATE wr_target AS d SET v=wr_writer(v) WHERE false AND 1=ANY(SELECT r.id FROM wr_source AS r WHERE r.id=d.id)", None, [], "UPDATE 0", None, 0),
        (hint + "DELETE FROM wr_target AS d WHERE d.id=ANY(SELECT CAST(CASE WHEN r.id=2 THEN '2' ELSE 'bad' END AS INT) FROM wr_source AS r WHERE r.id=d.id) RETURNING id", "22P02", [], None, None, 0),
        (hint + "UPDATE wr_target AS d SET v=(SELECT r.id FROM wr_source AS r WHERE r.id<=d.id) WHERE d.id=ANY(SELECT 2) OR d.id=ANY(SELECT 3) RETURNING id,v", "21000", [], None, None, 0),
        (hint + "UPDATE wr_target AS d SET v=wr_writer(v) WHERE false AND 1=ANY(SELECT r.missing_column FROM wr_source AS r WHERE r.id=d.id)", "42703", [], None, None, 0),
        ("WITH unused_writer AS(INSERT INTO wr_sink VALUES(wr_writer(99)) RETURNING id) UPDATE wr_target AS d SET v=1/0 WHERE d.id=ANY(SELECT r.id FROM wr_source AS r WHERE r.id=d.id)", "22012", [], None, None, 0),
        (hint + "DELETE FROM wr_target AS d WHERE d.id=ANY(SELECT r.id FROM wr_source AS r WHERE r.id=d.id) RETURNING id,(SELECT r.t FROM wr_target AS r WHERE r.id=d.id)", None, [["2", ""], ["3", "NULL"]], "DELETE 2", [23, 25], 0),
    ]
    initial = [["1", "10", None], ["2", "20", ""], ["3", "30", "NULL"], ["4", "40", "quoted ' space"]]
    try:
        for sql in ["BEGIN", "CREATE TEMP TABLE wr_target(id INT PRIMARY KEY,v INT,t TEXT)", "INSERT INTO wr_target VALUES(1,10,NULL),(2,20,''),(3,30,'NULL'),(4,40,'quoted '' space')", "CREATE TEMP TABLE wr_source(id INT)", "INSERT INTO wr_source VALUES(2),(3),(NULL)", "CREATE TEMP TABLE wr_sink(id INT)", "CREATE TEMP SEQUENCE wr_calls", "CREATE FUNCTION wr_writer(p INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('wr_calls'); INSERT INTO wr_sink VALUES(p); RETURN p; END$$"]:
            assert query(sql + ";")[1] is None
        expected = 0
        for sql, state, rows, tag, types, calls in cases:
            query("SAVEPOINT wr_case;")
            query(sql + ";", state, rows, tag, types)
            query("ROLLBACK TO wr_case;")
            query("SELECT id,v,t FROM wr_target ORDER BY id;", rows=initial, types=[23, 23, 25])
            query("SELECT id FROM wr_sink;", rows=[])
            expected += calls + 1
            query("SELECT nextval('wr_calls');", rows=[[str(expected)]], types=[20])
        query("ROLLBACK;")
        print("WITH RESTART FAILURES", failures, flush=True)
        assert not failures, failures
        print("[WITH PHYSICAL CHILD RESTART] passed whole 13-case matrix", flush=True)
    finally:
        try: c.simple_query(sock, "ROLLBACK;")
        finally:
            if server: r.stop_ours(server)
            else: sock.close()


if __name__ == "__main__": main()
