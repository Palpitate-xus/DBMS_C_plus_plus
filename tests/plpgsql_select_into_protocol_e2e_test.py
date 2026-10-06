#!/usr/bin/env python3
"""Stored PL/pgSQL SELECT INTO retains its query and typed first-row result."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("pl_into_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, headers, types):
        result = query(sql)
        assert result[1] is None, (sql, result)
        assert (result[0], result[3], result[4], result[5]) == (
            rows, headers, f"SELECT {len(rows)}", types), (sql, result)

    def function(name, result_type, body, arguments=""):
        sql = f"CREATE FUNCTION {name}({arguments}) RETURNS {result_type} LANGUAGE plpgsql AS $body$ {body} $body$;"
        assert query(sql)[1] is None, sql

    try:
        for sql in ("CREATE TABLE into_rows(id INT,payload TEXT);",
                    "INSERT INTO into_rows VALUES(1,'a b'),(2,NULL),(3,'null'),(4,''),(5,'O''Brien');"):
            assert query(sql)[1] is None, sql
        function("into_first", "INT", "DECLARE n INT; BEGIN SELECT id INTO n FROM into_rows ORDER BY id DESC; RETURN n; END;")
        expect("SELECT into_first() AS n;", [["5"]], ["n"], [23])
        function("into_read", "TEXT", "DECLARE v TEXT; BEGIN SELECT r.payload INTO v FROM into_rows r WHERE r.id=arg; RETURN v; END;", "arg INT")
        for arg, payload in ((1, "a b"), (2, None), (3, "null"), (4, ""), (5, "O'Brien"), (99, None)):
            expect(f"SELECT into_read({arg}) AS v;", [[payload]], ["v"], [25])
        function("into_scalar", "INT", "DECLARE n INT := NULL; BEGIN SELECT 99 INTO n; RETURN n; END;")
        expect("SELECT into_scalar() AS n;", [["99"]], ["n"], [23])
        function("into_none", "INT", "DECLARE n INT := 7; BEGIN SELECT id INTO n FROM into_rows WHERE id=99; RETURN n; END;")
        expect("SELECT into_none() AS n;", [[None]], ["n"], [23])
        function("into_pair", "TEXT", "DECLARE n INT; v TEXT; BEGIN SELECT r.id,r.payload INTO n,v FROM into_rows r WHERE r.id=1; RETURN v; END;")
        expect("SELECT into_pair() AS v;", [["a b"]], ["v"], [25])
        function("into_cte", "TEXT", "DECLARE v TEXT; BEGIN WITH c AS(SELECT id,payload FROM into_rows) SELECT payload INTO v FROM c WHERE id=1; RETURN v; END;")
        expect("SELECT into_cte() AS v;", [["a b"]], ["v"], [25])
        expect("WITH into_rows AS(SELECT 99 AS id,'caller' AS payload) SELECT into_read(1) AS v;",
               [["a b"]], ["v"], [25])
        function("into_join", "INT", "DECLARE n INT; BEGIN SELECT a.id+b.id INTO n FROM into_rows a CROSS JOIN into_rows b WHERE a.id=1 AND b.id=2; RETURN n; END;")
        expect("SELECT into_join() AS n;", [["3"]], ["n"], [23])
        function("into_alias", "INT", "DECLARE id INT := 99; n INT; BEGIN SELECT r.id INTO n FROM into_rows r WHERE r.id=1; RETURN n; END;")
        expect("SELECT into_alias() AS n;", [["1"]], ["n"], [23])
        function("into_relation", "INT", "DECLARE into_rows INT := 99; n INT; BEGIN SELECT id INTO n FROM into_rows WHERE id=1; RETURN n; END;")
        expect("SELECT into_relation() AS n;", [["1"]], ["n"], [23])
        function("into_echo", "TEXT", "DECLARE v TEXT; BEGIN SELECT arg INTO v; RETURN v; END;", "arg TEXT")
        for payload in ("null", "", "O'Brien", "00123", "1.000"):
            literal = "'" + payload.replace("'", "''") + "'"
            expect(f"SELECT into_echo({literal}) AS v;", [[payload]], ["v"], [25])
        expect("SELECT into_echo(NULL) AS v;", [[None]], ["v"], [25])
        function("into_null_expr", "TEXT", "DECLARE v TEXT; BEGIN SELECT NULL INTO v; RETURN v || 'x'; END;")
        expect("SELECT into_null_expr() AS v;", [[None]], ["v"], [25])
        function("into_null_cast", "TEXT", "DECLARE v TEXT; BEGIN SELECT NULL INTO v; RETURN CAST(v AS TEXT); END;")
        expect("SELECT into_null_cast() AS v;", [[None]], ["v"], [25])
        function("into_literal", "TEXT", "DECLARE v TEXT; BEGIN SELECT $$from;into$$ INTO v; RETURN v; END;")
        expect("SELECT into_literal() AS v;", [["from;into"]], ["v"], [25])
        function("into_after", "INT", "DECLARE n INT; BEGIN SELECT id FROM into_rows WHERE id=2 INTO n; RETURN n; END;")
        expect("SELECT into_after() AS n;", [["2"]], ["n"], [23])
        function("into_found", "BOOLEAN", "DECLARE n INT; BEGIN SELECT id INTO n FROM into_rows WHERE id=arg; RETURN FOUND; END;", "arg INT")
        expect("SELECT into_found(1) AS f,into_found(99) AS z;", [["t", "f"]], ["f", "z"], [16, 16])
        function("into_extra", "INT", "DECLARE n INT; BEGIN SELECT 7,'ignored' INTO n; RETURN n; END;")
        expect("SELECT into_extra() AS n;", [["7"]], ["n"], [23])
        function("into_short", "TEXT", "DECLARE n INT; v TEXT := 'old'; BEGIN SELECT 7 INTO n,v; RETURN v; END;")
        expect("SELECT into_short() AS v;", [[None]], ["v"], [25])
        function("into_cast_number", "TEXT", "DECLARE n INT; BEGIN SELECT 1.9 INTO n; RETURN CAST(n AS TEXT); END;")
        expect("SELECT into_cast_number() AS v;", [["2"]], ["v"], [25])
        function("into_cast_text", "TEXT", "DECLARE n INT; BEGIN SELECT arg INTO n; RETURN CAST(n AS TEXT); END;", "arg TEXT")
        expect("SELECT into_cast_text('123') AS v;", [["123"]], ["v"], [25])
        function("into_return_number", "INT", "BEGIN RETURN 1.9; END;")
        expect("SELECT into_return_number() AS n;", [["2"]], ["n"], [23])
        function("into_return_bad", "INT", "BEGIN RETURN '1.9'; END;")
        result = query("SELECT into_return_bad();")
        assert result[1] == "22P02" and result[0] == [], result
        for literal, state in (("'abc'", "22P02"), ("'1.9'", "22P02"), ("'2147483648'", "22003")):
            result = query(f"SELECT into_cast_text({literal});")
            assert result[1] == state and result[0] == [], (literal, result)
        function("into_strict", "INT", "DECLARE n INT; BEGIN SELECT id INTO STRICT n FROM into_rows WHERE id=arg; RETURN n; END;", "arg INT")
        expect("SELECT into_strict(1) AS n;", [["1"]], ["n"], [23])
        function("into_many", "INT", "DECLARE n INT; BEGIN SELECT id INTO STRICT n FROM into_rows; RETURN n; END;")
        for sql, state in (("SELECT into_strict(99);", "P0002"), ("SELECT into_many();", "P0003")):
            result = query(sql)
            assert result[1] == state and result[0] == [], (sql, result)
        function("into_missing", "INT", "DECLARE n INT; BEGIN SELECT id INTO n FROM no_such_into_relation; RETURN n; END;")
        result = query("SELECT into_missing();")
        assert result[1] == "42P01" and result[0] == [], result
        assert query("BEGIN")[1] is None
        result = query("SELECT into_many();")
        assert result[1] == "P0003", result
        result = query("SELECT into_first();")
        assert result[1] == "25P02", result
        assert query("ROLLBACK")[1] is None
        expect("SELECT into_first() AS n;", [["5"]], ["n"], [23])
        function("into_recursive", "INT", "DECLARE n INT; BEGIN IF arg<=0 THEN RETURN 0; END IF; SELECT into_recursive(arg-1) INTO n; RETURN n+1; END;", "arg INT")
        expect("SELECT into_recursive(3) AS n;", [["3"]], ["n"], [23])
        function("into_self", "INT", "DECLARE n INT; BEGIN SELECT into_self() INTO n; RETURN n; END;")
        result = query("SELECT into_self();")
        assert result[1] == "54001" and result[0] == [], result
        expect("SELECT into_recursive(3) AS n;", [["3"]], ["n"], [23])
        print("[PLPGSQL SELECT INTO PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
