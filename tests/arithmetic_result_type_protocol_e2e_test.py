#!/usr/bin/env python3
"""Projection types derive from the complete AST, including zero-row results."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("arithmetic_types_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    try:
        expect("SELECT (16777216::REAL+1::REAL) IS NOT DISTINCT FROM 16777216::REAL;", [["t"]], [16])
        for expression in ("CAST(1 AS REAL)+1", "1::REAL+1::INT", "CAST(1 AS REAL)+CAST(1 AS NUMERIC)"):
            expect("SELECT " + expression + ";", [["2"]], [701])
        expect("SELECT CAST(NULL AS REAL)+1;", [[None]], [701])
        expect("SELECT CAST(1 AS REAL)+CAST(1 AS REAL);", [["2"]], [700])
        expect("SELECT CAST(1 AS SMALLINT)+CAST(1 AS SMALLINT);", [["2"]], [21])
        expect("SELECT CAST(1 AS BIGINT)+1;", [["2"]], [20])
        expect("SELECT CAST((CAST(1 AS REAL)+1) AS REAL);", [["2"]], [700])
        expect("SELECT CASE WHEN CAST(1 AS REAL)>0 THEN 1 ELSE 2 END;", [["1"]], [23])
        for sql in ("CREATE TABLE ati(id INT,f REAL,i INT,n NUMERIC,b BIGINT,s SMALLINT);", "CREATE TABLE ati_other(id INT,f REAL);", "INSERT INTO ati VALUES(1,1,1,1,1,1);", "INSERT INTO ati_other VALUES(1,1);"):
            assert query(sql)[1] is None, sql
        # Casted operands reach the typed scalar evaluator; the separate
        # ordinary-column legacy arithmetic bridge is not claimed here.
        expect("SELECT f+CAST(1 AS REAL),f+CAST(1 AS INT),f+CAST(1 AS NUMERIC),b+CAST(1 AS INT),s+CAST(1 AS SMALLINT) FROM ati;", [["2", "2", "2", "2", "2"]], [700, 701, 701, 20, 21])
        expect("SELECT f+i,f+n,f+f,b+i,s+s,CAST(NULL AS REAL)+i FROM ati WHERE FALSE;", [], [701, 701, 700, 20, 21, 701])
        expect("SELECT l.f+CAST(1 AS REAL),l.f+CAST(1 AS INT),(l.f+CAST(1 AS REAL)) IS NOT DISTINCT FROM r.f FROM ati l JOIN ati_other r ON l.id=r.id;", [["2", "2", "f"]], [700, 701, 16])
        expect("SELECT l.f+r.f,l.f+l.i FROM ati l JOIN ati_other r ON FALSE;", [], [700, 701])
        print("[ARITHMETIC RESULT TYPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
