#!/usr/bin/env python3
"""Malformed ALTER DATABASE RENAME must not move a database directory."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "alter_database_rename_trailing_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    old = "rename_trailing_source"
    new = "rename_trailing_target"

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    try:
        rows, state, message, _, _, _ = query("CREATE DATABASE " + old + ";")
        assert state is None, (rows, state, message)
        data_dir = Path(server["dir"])
        rows, state, message, _, _, _ = query(
            f"ALTER DATABASE {old} RENAME TO {new} unexpected;")
        assert state == "42601", (rows, state, message)
        assert (data_dir / old).is_dir()
        assert not (data_dir / new).exists()

        rows, state, message, _, _, _ = query(
            f"ALTER DATABASE {old} RENAME TO {new};")
        assert state is None, (rows, state, message)
        assert not (data_dir / old).exists()
        assert (data_dir / new).is_dir()
        rows, state, message, _, _, _ = query("DROP DATABASE " + new + ";")
        assert state is None, (rows, state, message)
        print("[ALTER DATABASE RENAME TRAILING TOKENS E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
