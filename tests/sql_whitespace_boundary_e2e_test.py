#!/usr/bin/env python3
"""SQL whitespace separates tokens while quoted whitespace remains data."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("whitespace_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ("CREATE TABLE whitespace_rows (id INT);",
                    "INSERT INTO whitespace_rows VALUES (1), (2), (3);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
        cases = [("SELECT\nid\tFROM whitespace_rows\rWHERE id = 1;", [["1"]]),
                 ("SELECT id FROM whitespace_rows\nORDER\r\nBY id DESC\nFETCH\tFIRST 1\tROW ONLY;", [["3"]])]
        for whitespace in ("\n", "\t", "\r", "  "):
            cases.append(("SELECT length('a" + whitespace + "b') AS n FROM whitespace_rows WHERE id = 1;",
                          [[str(2 + len(whitespace))]]))
        for sql, expected in cases:
            rows, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None and rows == expected, (sql, state, message, rows, expected)
        print("[SQL WHITESPACE BOUNDARY E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
