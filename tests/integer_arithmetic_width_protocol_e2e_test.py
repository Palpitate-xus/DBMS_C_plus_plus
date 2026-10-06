#!/usr/bin/env python3
"""Integer arithmetic honors the resolved int2/int4/int8 operator width."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("integer_width_runner", root / "tests/compat/pg_diff_runner.py")
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
        for sql in ("CREATE TABLE iw_l(id INT,i INT,s SMALLINT,b BIGINT);", "CREATE TABLE iw_r(id INT,i INT,s SMALLINT,b BIGINT);", "INSERT INTO iw_l VALUES(1,2147483647,32767,9223372036854775807),(2,NULL,NULL,NULL);", "INSERT INTO iw_r VALUES(10,2147483647,32767,9223372036854775807),(20,NULL,NULL,NULL);"):
            assert query(sql)[1] is None, sql
        pair = [["1", "10"], ["2", "20"]]
        rows("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON l.i+0 IS NOT DISTINCT FROM r.i ORDER BY l.id;", pair)
        rows("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON l.b+0 IS NOT DISTINCT FROM r.b ORDER BY l.id;", pair)
        error("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON l.i+1 IS NOT DISTINCT FROM r.i;", "22003")
        error("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON l.s+CAST(1 AS SMALLINT) IS NOT DISTINCT FROM r.s;", "22003")
        error("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON l.b+1 IS NOT DISTINCT FROM r.b;", "22003")
        rows("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON l.i+CAST(1 AS BIGINT) IS NOT DISTINCT FROM CAST(r.i AS BIGINT)+1 ORDER BY l.id;", pair)
        rows("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON l.s+1 IS NOT DISTINCT FROM CAST(r.s AS INT)+1 ORDER BY l.id;", pair)
        for expression in ("CAST(2147483647 AS INT)+1", "CAST(-2147483648 AS INT)/-1", "CAST(32767 AS SMALLINT)+CAST(1 AS SMALLINT)", "CAST(-32768 AS SMALLINT)/CAST(-1 AS SMALLINT)", "-CAST(-2147483648 AS INT)", "-CAST(-32768 AS SMALLINT)"):
            error("SELECT " + expression + ";", "22003")
        error("SELECT CAST(1 AS INT)/0;", "22012")
        error("SELECT CAST(1 AS INT)+'oops';", "22P02")
        assert query("SELECT CAST(2147483647 AS INT)+CAST(1 AS BIGINT);")[0] == [["2147483648"]]
        assert query("CREATE FUNCTION iw_big(v BIGINT) RETURNS BIGINT LANGUAGE plpgsql AS $$BEGIN RETURN v+0; END;$$;")[1] is None
        rows("SELECT l.id,r.id FROM iw_l l JOIN iw_r r ON iw_big(l.b) IS NOT DISTINCT FROM r.b ORDER BY l.id;", pair)
        print("[INTEGER ARITHMETIC WIDTH PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
