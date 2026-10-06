#!/usr/bin/env python3
"""COLLATE labels never enter the PL/pgSQL data-variable namespace."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("collation_label_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    def function(name, body):
        sql = f"CREATE FUNCTION {name}() RETURNS TEXT LANGUAGE plpgsql AS $body$ {body} $body$;"
        result = query(sql)
        assert result[1] is None, (sql, result)

    try:
        # Basic SQL is the positive control for the stored-statement path.
        expect('SELECT \'a\' COLLATE "C";', [["a"]], [25])
        function("collate_name", 'DECLARE "C" TEXT := \'bad\'; value TEXT := \'a\'; n TEXT; BEGIN SELECT value COLLATE "C" INTO n; RETURN n; END;')
        expect("SELECT collate_name();", [["a"]], [25])
        function("collate_comment", 'DECLARE "C" TEXT := \'bad\'; n TEXT; BEGIN SELECT \'a\' COLLATE /* label */ "C" INTO n; RETURN n; END;')
        expect("SELECT collate_comment();", [["a"]], [25])
        function("collate_null", 'DECLARE "C" TEXT := \'bad\'; value TEXT; n TEXT; BEGIN SELECT value COLLATE "C" INTO n; RETURN n; END;')
        expect("SELECT collate_null();", [[None]], [25])
        function("collate_data", 'DECLARE "C" TEXT := \'bad\'; n TEXT; BEGIN SELECT \'a\'||"C" INTO n; RETURN n; END;')
        expect("SELECT collate_data();", [["abad"]], [25])
        function("collate_unknown", 'DECLARE "NoSuchCollation" TEXT := \'C\'; n TEXT; BEGIN SELECT \'a\' COLLATE "NoSuchCollation" INTO n; RETURN n; END;')
        result = query("SELECT collate_unknown();")
        assert result[1] == "42704" and "NoSuchCollation".lower() in result[2].lower(), result
        expect("SELECT collate_name(),collate_data();", [["a", "abad"]], [25, 25])
        print("[PLPGSQL COLLATION LABEL PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
