#!/usr/bin/env python3
"""Aggregate arithmetic uses the filtered aggregate scope, including empty input."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("aggregate_scope_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in ('CREATE TABLE aggregate_scope_rows(v INT);',
                    'INSERT INTO aggregate_scope_rows VALUES(1),(NULL),(2);',
                    'CREATE TABLE aggregate_scope_empty(v INT);',
                    'CREATE TABLE aggregate_scope_null(v INT);',
                    'INSERT INTO aggregate_scope_null VALUES(NULL);'):
            assert query(sql)[1] is None, sql
        for sql, rows in (
            ('SELECT count(*)-count(v) FROM aggregate_scope_rows;', [["1"]]),
            ('SELECT sum(v)+1 FROM aggregate_scope_rows;', [["4"]]),
            ('SELECT count(*)-count(v) FROM aggregate_scope_empty;', [["0"]]),
            ('SELECT sum(v)+1 FROM aggregate_scope_empty;', [[None]]),
            ('SELECT sum(v)+1 FROM aggregate_scope_null;', [[None]]),
            ('SELECT sum(v)+1 FROM aggregate_scope_rows WHERE v=1;', [["2"]]),
            ('SELECT CAST(sum(v) AS BIGINT)+1 FROM aggregate_scope_rows;', [["4"]]),
        ):
            result = query(sql)
            assert result[1] is None and result[0] == rows and result[5] == [20], (sql, result)
        print("[AGGREGATE ARITHMETIC SCOPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
