#!/usr/bin/env python3
"""REAL and DOUBLE arithmetic is floating point, not exact NUMERIC."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("floating_width_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def rows(sql, expected):
        result = query(sql)
        assert result[1] is None and result[0] == expected and result[5] == [23, 23], (sql, result)

    def error(sql, state):
        result = query(sql)
        assert result[1] == state, (sql, result)

    try:
        for sql in ("CREATE TABLE fw_l(id INT,f REAL,d DOUBLE PRECISION);", "CREATE TABLE fw_r(id INT,f REAL,d DOUBLE PRECISION);", "INSERT INTO fw_l VALUES(1,16777216,9007199254740992),(2,NULL,NULL);", "INSERT INTO fw_r VALUES(10,16777216,9007199254740992),(20,NULL,NULL);"):
            assert query(sql)[1] is None, sql
        pair = [["1", "10"], ["2", "20"]]
        rows("SELECT l.id,r.id FROM fw_l l JOIN fw_r r ON l.f IS NOT DISTINCT FROM r.f ORDER BY l.id;", pair)
        rows("SELECT l.id,r.id FROM fw_l l JOIN fw_r r ON l.f+CAST(1 AS REAL) IS NOT DISTINCT FROM r.f ORDER BY l.id;", pair)
        rows("SELECT l.id,r.id FROM fw_l l JOIN fw_r r ON l.d+CAST(1 AS DOUBLE PRECISION) IS NOT DISTINCT FROM r.d ORDER BY l.id;", pair)
        rows("SELECT l.id,r.id FROM fw_l l JOIN fw_r r ON l.f+1 IS NOT DISTINCT FROM CAST(r.f AS DOUBLE PRECISION)+1 ORDER BY l.id;", pair)
        rows("SELECT l.id,r.id FROM fw_l l LEFT JOIN fw_r r ON l.f+CAST(1 AS REAL) IS DISTINCT FROM r.f ORDER BY l.id,r.id;", [["1", "20"], ["2", "10"]])
        assert query("CREATE FUNCTION fw_real(v REAL) RETURNS REAL LANGUAGE plpgsql AS $$BEGIN RETURN v+CAST(1 AS REAL); END;$$;")[1] is None
        assert query("CREATE FUNCTION fw_double(v DOUBLE PRECISION) RETURNS DOUBLE PRECISION LANGUAGE plpgsql AS $$BEGIN RETURN v+CAST(1 AS DOUBLE PRECISION); END;$$;")[1] is None
        rows("SELECT l.id,r.id FROM fw_l l JOIN fw_r r ON fw_real(l.f) IS NOT DISTINCT FROM r.f ORDER BY l.id;", pair)
        rows("SELECT l.id,r.id FROM fw_l l JOIN fw_r r ON fw_double(l.d) IS NOT DISTINCT FROM r.d ORDER BY l.id;", pair)
        for expression, state in (("CAST(3.4028235e38 AS REAL)*CAST(2 AS REAL)", "22003"), ("CAST(1.7976931348623157e308 AS DOUBLE PRECISION)*CAST(2 AS DOUBLE PRECISION)", "22003"), ("CAST('1e-45' AS REAL)/CAST(2 AS REAL)", "22003"), ("CAST(1 AS REAL)/CAST(0 AS REAL)", "22012"), ("CAST(1 AS REAL)%CAST(1 AS REAL)", "42883")):
            error("SELECT " + expression + ";", state)
        rows("SELECT l.id,r.id FROM fw_l l JOIN fw_r r ON CASE WHEN l.id=1 THEN l.f+CAST(1 AS REAL) ELSE l.f END IS NOT DISTINCT FROM r.f ORDER BY l.id;", pair)
        print("[FLOATING ARITHMETIC WIDTH PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
