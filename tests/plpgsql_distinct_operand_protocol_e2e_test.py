#!/usr/bin/env python3
"""IS [NOT] DISTINCT FROM is an operator unit, not a relation clause."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("distinct_operand_runner", root / "tests/compat/pg_diff_runner.py")
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

    try:
        expect("SELECT 1 IS DISTINCT FROM 2,1 IS NOT DISTINCT FROM 1,NULL IS DISTINCT FROM NULL;",
               [["t", "t", "f"]], [16, 16, 16])
        function("distinct_variable", "BOOLEAN", "DECLARE wanted INT := 2; n BOOLEAN; BEGIN SELECT 1 IS DISTINCT FROM wanted INTO n; RETURN n; END;")
        expect("SELECT distinct_variable();", [["t"]], [16])
        function("distinct_null", "BOOLEAN", "DECLARE wanted INT; n BOOLEAN; BEGIN SELECT NULL IS NOT DISTINCT FROM wanted INTO n; RETURN n; END;")
        expect("SELECT distinct_null();", [["t"]], [16])
        function("distinct_quoted", "BOOLEAN", 'DECLARE "Wanted" INT := 1; wanted INT := 2; n BOOLEAN; BEGIN SELECT 1 IS NOT DISTINCT FROM "Wanted" INTO n; RETURN n; END;')
        expect("SELECT distinct_quoted();", [["t"]], [16])
        function("distinct_operator_name", "BOOLEAN", 'DECLARE "from" INT := 2; n BOOLEAN; BEGIN SELECT 1 IS /* op */ NOT DISTINCT /* op */ FROM "from" INTO n; RETURN n; END;')
        expect("SELECT distinct_operator_name();", [["f"]], [16])
        function("distinct_expression", "BOOLEAN", "DECLARE wanted INT := 2; n BOOLEAN; BEGIN SELECT 3 IS NOT DISTINCT FROM (wanted+1) AS wanted INTO n; RETURN n; END;")
        expect("SELECT distinct_expression();", [["t"]], [16])
        function("distinct_direct", "BOOLEAN", "DECLARE wanted INT := 2; BEGIN RETURN 1 IS DISTINCT FROM wanted; END;")
        expect("SELECT distinct_direct();", [["t"]], [16])
        # Actual source-table composition is retained in the independent
        # distinct_source_query test: main's historic normalization of a
        # complex RHS was a separate downstream failure of that pipeline.
        function("distinct_unknown", "BOOLEAN", "DECLARE n BOOLEAN; BEGIN SELECT 1 IS DISTINCT FROM no_such_variable INTO n; RETURN n; END;")
        result = query("SELECT distinct_unknown();")
        assert result[1] == "42703" and result[0] == [], result
        expect("SELECT distinct_variable(),distinct_null();", [["t", "t"]], [16, 16])
        print("[PLPGSQL DISTINCT OPERAND PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
