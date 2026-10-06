#!/usr/bin/env python3
"""Stored scalar expressions preserve delimited variable identity and NULL/type."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("quoted_binding_runner", root / "tests/compat/pg_diff_runner.py")
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
        assert query(sql)[1] is None, sql

    try:
        function("quoted_sum", "INT", 'DECLARE "X" INT := 1; x INT := 2; BEGIN RETURN "X"+x; END;')
        expect("SELECT quoted_sum() AS n;", [["3"]], [23])
        function("quoted_bigint", "BIGINT", 'DECLARE "X" BIGINT := 5000000000; x INT := 2; BEGIN RETURN "X"+x; END;')
        expect("SELECT quoted_bigint() AS n;", [["5000000002"]], [20])
        function("quoted_null", "INT", 'DECLARE "X" INT; x INT := 2; BEGIN RETURN coalesce("X",9)+x; END;')
        expect("SELECT quoted_null() AS n;", [["11"]], [23])
        function("quoted_type", "TEXT", 'DECLARE "X" TEXT := \'00123\'; x INT := 2; BEGIN RETURN "X"||\':\'||CAST(x AS TEXT); END;')
        expect("SELECT quoted_type() AS v;", [["00123:2"]], [25])
        function("quoted_case", "INT", 'DECLARE "X" INT := 1; x INT := 2; BEGIN RETURN CASE WHEN "X"=1 THEN x+10 ELSE 99 END; END;')
        expect("SELECT quoted_case() AS n;", [["12"]], [23])
        function("quoted_space", "INT", 'DECLARE "Value Name" INT := 3; "Q""Col" INT := 4; BEGIN RETURN "Value Name"*10+"Q""Col"; END;')
        expect("SELECT quoted_space() AS n;", [["34"]], [23])
        function("quoted_private", "INT", 'DECLARE "__plpgsql_variable_0" INT := 3; "X" INT := 1; x INT := 2; BEGIN RETURN "__plpgsql_variable_0"+"X"+x; END;')
        expect("SELECT quoted_private() AS n;", [["6"]], [23])
        function("quoted_unknown", "INT", "BEGIN RETURN no_such_variable+1; END;")
        function("quoted_qualifier", "INT", "DECLARE x INT := 2; BEGIN RETURN missing.x+1; END;")
        function("quoted_function", "INT", 'DECLARE "X" INT := 1; BEGIN RETURN no_such_function("X"); END;')
        function("quoted_division", "INT", 'DECLARE "X" INT := 1; BEGIN RETURN "X"/0; END;')
        for name, state in (("quoted_unknown", "42703"), ("quoted_qualifier", "42P01"),
                            ("quoted_function", "42883"), ("quoted_division", "22012")):
            result = query(f"SELECT {name}();")
            assert result[1] == state and result[0] == [], (name, result)
        expect("SELECT quoted_sum() AS n,quoted_null() AS v;", [["3", "11"]], [23, 23])
        print("[PLPGSQL QUOTED SCALAR BINDING PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
