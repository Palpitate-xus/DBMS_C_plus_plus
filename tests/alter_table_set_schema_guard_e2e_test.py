#!/usr/bin/env python3
"""SET SCHEMA must not move relation files into a same-named database."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "set_schema_guard_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        expect("CREATE SCHEMA move_target;", None)
        expect("CREATE DATABASE move_target;", None)
        expect("CREATE TABLE move_items (id INT);", None)
        expect("INSERT INTO move_items VALUES (7);", None)
        expect("ALTER TABLE move_items SET SCHEMA move_target;", "0A000")
        rows = expect("SELECT id FROM move_items;", None)
        assert rows == [["7"]], rows
        data_dir = Path(server["dir"])
        assert (data_dir / "info" / "move_items.stc").exists()
        assert not (data_dir / "move_target" / "move_items.stc").exists()
        expect("DROP TABLE move_items;", None)
        expect("DROP DATABASE move_target;", None)
        expect("DROP SCHEMA move_target;", None)
        print("[ALTER TABLE SET SCHEMA GUARD E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
