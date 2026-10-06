#!/usr/bin/env python3
"""Source-query null-safe comparisons retain full typed operand ASTs."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("distinct_source_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    def function(name, kind, body):
        sql = f"CREATE FUNCTION {name}() RETURNS {kind} LANGUAGE plpgsql AS $body$ {body} $body$;"
        result = query(sql)
        assert result[1] is None, (sql, result)

    def command(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    try:
        assert query("CREATE TABLE distinct_source(id INT,payload TEXT);")[1] is None
        assert query("INSERT INTO distinct_source VALUES(1,'a'),(2,'b'),(3,NULL),(NULL,'c');")[1] is None
        # Positive control: simple operands and actual FROM/alias resolution.
        expect("SELECT r.id IS DISTINCT FROM 2 FROM distinct_source r WHERE r.id=1;", [["t"]], [16])
        expect("SELECT r.id IS DISTINCT FROM CAST('2' AS INT) FROM distinct_source r WHERE r.id=1;", [["t"]], [16])
        expect("SELECT r.id FROM distinct_source r WHERE r.id IS NOT DISTINCT FROM CAST('2' AS INT);", [["2"]], [23])
        expect("SELECT (r.id IS DISTINCT FROM 2) FROM distinct_source r WHERE r.id=1;", [["t"]], [16])
        expect("SELECT (r.id IS DISTINCT FROM CAST('2' AS INT)) FROM distinct_source r WHERE r.id=1;", [["t"]], [16])
        expect("SELECT id IS DISTINCT FROM CAST('2' AS INT) FROM distinct_source ORDER BY id;", [["t"], ["f"], ["t"], ["t"]], [16])
        expect("SELECT id,id IS NOT DISTINCT FROM 2 AS same FROM distinct_source ORDER BY same DESC,id;", [["2", "t"], ["1", "f"], ["3", "f"], [None, "f"]], [23, 16])
        expect("SELECT id FROM distinct_source WHERE id IS DISTINCT FROM CAST('2' AS INT) ORDER BY id;", [["1"], ["3"], [None]], [23])
        expect("SELECT id FROM distinct_source WHERE id IS NOT DISTINCT FROM CAST(NULL AS INT);", [[None]], [23])
        expect("SELECT id FROM distinct_source WHERE id IS NOT DISTINCT FROM abs(-2);", [["2"]], [23])
        expect("SELECT id FROM distinct_source WHERE id IS NOT DISTINCT FROM CASE WHEN TRUE THEN 2 ELSE 99 END;", [["2"]], [23])
        expect("SELECT id IS NOT DISTINCT FROM CASE WHEN id=2 THEN id ELSE 2 END FROM distinct_source ORDER BY id;", [["f"], ["t"], ["f"], ["f"]], [16])
        expect("SELECT id FROM distinct_source WHERE (id IS NOT DISTINCT FROM (1+1)) AND payload IS NOT NULL;", [["2"]], [23])
        # Actual JOIN ranges are preserved; the existing unsupported null-safe
        # JOIN ON family is retained in the separate source diagnostic probe.
        expect("SELECT a.id,b.id FROM distinct_source a JOIN distinct_source b ON a.id=b.id ORDER BY a.id,b.id;", [["1", "1"], ["2", "2"], ["3", "3"]], [23, 23])
        expect("SELECT a.id IS NOT DISTINCT FROM b.id FROM distinct_source a CROSS JOIN distinct_source b WHERE a.id=1 AND b.id=2;", [["f"]], [16])
        expect("SELECT (NULL::INT IS NOT DISTINCT FROM NULL::INT),(NULL::INT IS DISTINCT FROM 2);", [["t", "t"]], [16, 16])
        for sql in ("SELECT id IS DISTINCT FROM CAST('bad' AS INT) FROM distinct_source;",
                    "SELECT id FROM distinct_source WHERE id IS NOT DISTINCT FROM CAST('bad' AS INT);"):
            result = query(sql)
            assert result[1] == "22P02" and result[0] == [], (sql, result)
        function("distinct_source_projection", "BOOLEAN", "DECLARE wanted INT := 2; n BOOLEAN; BEGIN SELECT r.id IS DISTINCT FROM wanted INTO n FROM distinct_source r WHERE r.id=1; RETURN n; END;")
        function("distinct_source_where", "INT", "DECLARE wanted INT := 2; n INT; BEGIN SELECT r.id INTO n FROM distinct_source r WHERE r.id IS NOT DISTINCT FROM wanted; RETURN n; END;")
        expect("SELECT distinct_source_projection();", [["t"]], [16])
        expect("SELECT distinct_source_where();", [["2"]], [23])
        function("distinct_source_refill", "BOOLEAN", "DECLARE wanted INT := 2; n BOOLEAN; BEGIN SELECT r.id IS DISTINCT FROM wanted INTO n FROM distinct_source r WHERE FALSE; SELECT r.id IS DISTINCT FROM wanted INTO n FROM distinct_source r WHERE r.id=1; RETURN n; END;")
        function("distinct_source_empty", "BOOLEAN", "DECLARE wanted INT := 2; n BOOLEAN := TRUE; BEGIN SELECT r.id IS DISTINCT FROM wanted INTO n FROM distinct_source r WHERE FALSE; RETURN n; END;")
        expect("SELECT distinct_source_refill(),distinct_source_empty();", [["t", None]], [16, 16])
        assert query("CREATE TABLE distinct_mutation(id INT);")[1] is None
        assert query("INSERT INTO distinct_mutation VALUES(1),(2),(NULL);")[1] is None
        command("UPDATE distinct_mutation SET id=12 WHERE id IS NOT DISTINCT FROM CAST('2' AS INT);")
        expect("SELECT id FROM distinct_mutation ORDER BY id;", [["1"], ["12"], [None]], [23])
        command("DELETE FROM distinct_mutation WHERE id IS NOT DISTINCT FROM CAST(NULL AS INT);")
        expect("SELECT id FROM distinct_mutation ORDER BY id;", [["1"], ["12"]], [23])
        expect("SELECT distinct_source_projection(),distinct_source_where();", [["t", "2"]], [16, 23])
        print("[DISTINCT SOURCE QUERY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
