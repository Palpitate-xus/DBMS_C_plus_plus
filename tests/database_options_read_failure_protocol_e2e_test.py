#!/usr/bin/env python3
"""Unreadable or malformed options must not be replaced by partial metadata."""

import importlib.util
import os
from pathlib import Path
import stat


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "database_options_read_failure_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    first = "options_read_failure_first"
    second = "options_read_failure_second"
    options = Path(server["dir"]) / ".pg_database_options"
    original_mode = None

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    def good(sql):
        rows, state, message, _, _, _ = query(sql)
        assert state is None, (sql, rows, state, message)

    try:
        for name in (first, second):
            good("CREATE DATABASE " + name + ";")
            good(f"ALTER DATABASE {name} SET test_option TO '{name}';")
        original = options.read_bytes()
        assert first.encode() in original and second.encode() in original
        original_mode = stat.S_IMODE(options.stat().st_mode)

        os.chmod(options, 0)
        rows, state, message, _, _, _ = query(
            f"ALTER DATABASE {first} SET test_option TO 'changed';")
        assert state == "58030", (rows, state, message)
        os.chmod(options, original_mode)
        assert options.read_bytes() == original

        malformed = original + b"malformed_record\n"
        options.write_bytes(malformed)
        rows, state, message, _, _, _ = query(
            f"ALTER DATABASE {first} SET test_option TO 'changed';")
        assert state == "58030", (rows, state, message)
        assert options.read_bytes() == malformed

        duplicate = original + original.splitlines(keepends=True)[0]
        options.write_bytes(duplicate)
        rows, state, message, _, _, _ = query(
            f"ALTER DATABASE {first} SET test_option TO 'changed';")
        assert state == "58030", (rows, state, message)
        assert options.read_bytes() == duplicate

        options.write_bytes(original)
        good(f"ALTER DATABASE {first} SET test_option TO 'changed';")
        contents = options.read_bytes()
        assert b"changed" in contents and second.encode() in contents
        print("[DATABASE OPTIONS READ FAILURE PROTOCOL E2E] passed")
    finally:
        if original_mode is not None and options.exists():
            os.chmod(options, original_mode)
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
