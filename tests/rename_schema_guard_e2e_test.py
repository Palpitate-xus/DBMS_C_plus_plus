#!/usr/bin/env python3
"""ALTER SCHEMA RENAME must fail closed until namespace migration is atomic."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "rename_schema_guard_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, expected_state):
        rows, state, message, _, _, _ = query(sql)
        assert state == expected_state, (sql, rows, state, message)
        return rows

    try:
        expect("CREATE SCHEMA old_space;", None)
        expect("CREATE TABLE old_space.items (id INT);", None)
        expect("INSERT INTO old_space.items VALUES (7);", None)
        expect("ALTER SCHEMA old_space RENAME TO new_space;", "0A000")
        assert expect("SELECT id FROM old_space.items;", None) == [["7"]]
        data_dir = Path(server["dir"])
        assert (data_dir / "info" / ".schema_old_space").exists()
        assert not (data_dir / "info" / ".schema_new_space").exists()
        assert (data_dir / "info" / "old_space__items.stc").exists()
        assert not (data_dir / "info" / "new_space__items.stc").exists()
        expect("DROP TABLE old_space.items;", None)
        expect("DROP SCHEMA old_space;", None)
        print("[RENAME SCHEMA GUARD E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
