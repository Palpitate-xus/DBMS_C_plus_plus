#!/usr/bin/env python3
"""SQL database DDL must not delete or overwrite non-database paths."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "database_directory_guard_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    data_dir = Path(server["dir"])
    ordinary = "path_guard_ordinary"
    alias = "path_guard_alias"
    occupied = "path_guard_occupied"
    valid = "path_guard_valid"

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, expected_state):
        rows, state, message, _, _, _ = query(sql)
        assert state == expected_state, (sql, rows, state, message)

    try:
        expect("CREATE DATABASE " + valid + ";", None)
        (data_dir / ordinary).mkdir()
        sentinel = data_dir / ordinary / "keep.txt"
        sentinel.write_text("unrelated data")
        expect("DROP DATABASE " + ordinary + ";", "3D000")
        expect("CREATE DATABASE " + ordinary + ";", "42P04")
        assert sentinel.read_text() == "unrelated data"

        (data_dir / alias).symlink_to(valid, target_is_directory=True)
        expect("DROP DATABASE " + alias + ";", "3D000")
        expect(f"ALTER DATABASE {valid} RENAME TO {alias};", "42P04")
        assert (data_dir / alias).is_symlink()
        assert (data_dir / valid).is_dir()

        (data_dir / occupied).mkdir()
        expect(f"ALTER DATABASE {valid} RENAME TO {occupied};", "42P04")
        assert (data_dir / valid).is_dir()
        assert (data_dir / occupied).is_dir()
        expect("DROP DATABASE " + valid + ";", None)
        print("[DATABASE DIRECTORY GUARD PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
