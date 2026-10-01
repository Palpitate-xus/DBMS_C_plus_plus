#!/usr/bin/env python3
"""Long default SQL survives schema reload, rewrite and transaction rollback."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "long_default_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None):
        rows, state, message, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    try:
        original, changed, transient = "a" * 180, "b" * 260, "c" * 300
        query(f"CREATE TABLE long_default(id INT,value TEXT NOT NULL DEFAULT '{original}');")
        query("INSERT INTO long_default(id) VALUES(1) RETURNING value;", [[original]])
        query(f"ALTER TABLE long_default ALTER COLUMN value SET DEFAULT '{changed}';")
        query("ALTER TABLE long_default RENAME COLUMN value TO renamed;")
        query("INSERT INTO long_default(id) VALUES(2) RETURNING renamed;", [[changed]])
        query("BEGIN;")
        query("SAVEPOINT long_default_sp;")
        query(f"ALTER TABLE long_default ALTER COLUMN renamed SET DEFAULT '{transient}';")
        query("INSERT INTO long_default(id) VALUES(3) RETURNING renamed;", [[transient]])
        query("ROLLBACK TO SAVEPOINT long_default_sp;")
        query("INSERT INTO long_default(id) VALUES(4) RETURNING renamed;", [[changed]])
        query("COMMIT;")
        query("SELECT id,renamed FROM long_default ORDER BY id;", [["1", original], ["2", changed], ["4", changed]])
        query("ALTER TABLE long_default ALTER COLUMN renamed DROP DEFAULT;")
        _, state, message, _, _ = runner.decode_wire_result(client.simple_query(
            server["sock"], "INSERT INTO long_default(id) VALUES(5);"))
        assert state == "23502", (state, message)
        query("DROP TABLE long_default;")
        print("[LONG DEFAULT EXPRESSION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
