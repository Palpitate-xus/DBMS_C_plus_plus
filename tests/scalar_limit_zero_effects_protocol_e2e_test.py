#!/usr/bin/env python3
"""Ordinary scalar LIMIT 0 must not execute qualification or projection."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("scalar_limit_zero_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)
    try:
        for sql in (
            "CREATE TABLE scalar_zero_rows(v INT);",
            "INSERT INTO scalar_zero_rows VALUES(1),(NULL),(2);",
            "CREATE TABLE scalar_zero_effect(v INT);",
            "CREATE FUNCTION scalar_zero_writer(i INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO scalar_zero_effect VALUES(i); RETURN i; END;$$;",
        ):
            assert query(sql)[1] is None, sql
        for sql in (
            "SELECT scalar_zero_writer(v)+1 FROM scalar_zero_rows LIMIT 0;",
            "SELECT scalar_zero_writer(v)+1 FROM scalar_zero_rows ORDER BY scalar_zero_writer(v)+1 LIMIT 0;",
            "SELECT scalar_zero_writer(v)+1 FROM scalar_zero_rows WHERE scalar_zero_writer(v)=1 LIMIT 0;",
        ):
            result = query(sql)
            assert result[1] is None and result[0] == [] and result[5] == [23], (sql, result)
            effects = query("SELECT v FROM scalar_zero_effect;")
            assert effects[1] is None and effects[0] == [], (sql, effects)
        # Nonzero demand still consumes every ordinary row and its typed NULL.
        result = query("SELECT scalar_zero_writer(v)+1 FROM scalar_zero_rows;")
        assert result[1] is None and result[0] == [["2"], [None], ["3"]] and result[5] == [23], result
        effects = query("SELECT v FROM scalar_zero_effect;")
        assert effects[1] is None and effects[0] == [["1"], [None], ["2"]], effects
        print("[SCALAR LIMIT ZERO EFFECTS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
