#!/usr/bin/env python3
"""Boolean lowering cannot change strings or identifier components."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("boolean_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ("CREATE TABLE literal_flags (is_true_flag INT, false_count INT);",
                    "INSERT INTO literal_flags VALUES (7, 8);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
        for literal, expected in [("'true false'", "true false"),
                                  ("'TRUE FALSE'", "TRUE FALSE"),
                                  ("'it''s true'", "it's true"),
                                  ("'nottrue falsehood'", "nottrue falsehood")]:
            sql = "SELECT " + literal + " AS value FROM literal_flags;"
            rows, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None and rows == [[expected]], (sql, state, message, rows)
        sql = "SELECT is_true_flag, false_count FROM literal_flags;"
        rows, state, message, headers = runner.ours_query(client, server["sock"], sql)
        assert state is None and rows == [["7", "8"]], (sql, state, message, rows)
        assert headers == ["is_true_flag", "false_count"], headers
        print("[BOOLEAN LITERAL BOUNDARY E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
