#!/usr/bin/env python3
"""Malformed DROP lists must report syntax errors without removing targets."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "drop_list_syntax_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    try:
        query("CREATE TABLE drop_list_a(id INT);")
        query("CREATE TABLE drop_list_b(id INT);")
        query("INSERT INTO drop_list_a VALUES(1);")
        query("INSERT INTO drop_list_b VALUES(2);")
        for sql in ["DROP TABLE drop_list_a drop_list_b;", "DROP TABLE drop_list_a,;",
                    "DROP TABLE ,drop_list_a;", "DROP TABLE drop_list_a,,drop_list_b;",
                    "DROP TABLE drop_list_a CASCADE drop_list_b;",
                    "DROP TABLE drop_list_a RESTRICT CASCADE;"]:
            _, state, message, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
            assert state == "42601", (sql, state, message)
            assert query("SELECT id FROM drop_list_a;") == [["1"]]
            assert query("SELECT id FROM drop_list_b;") == [["2"]]
        print("[DROP TABLE LIST SYNTAX PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
