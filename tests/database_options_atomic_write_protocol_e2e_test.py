#!/usr/bin/env python3
"""A failed database-option update must leave the previous file intact."""

import importlib.util
import os
from pathlib import Path
import stat


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "database_options_atomic_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    data_dir = Path(server["dir"])
    original_mode = stat.S_IMODE(data_dir.stat().st_mode)
    database = "options_atomic_write"

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    def good(sql):
        rows, state, message, _, _, _ = query(sql)
        assert state is None, (sql, rows, state, message)

    try:
        good("CREATE DATABASE " + database + ";")
        good("ALTER DATABASE " + database + " OWNER TO alice;")
        options = data_dir / ".pg_database_options"
        original = options.read_bytes()
        assert original and b"alice" in original

        # The existing file remains writable, but publishing a replacement
        # needs a new directory entry. A truncate-in-place implementation
        # falsely reports success (and changes the previous bytes).
        os.chmod(data_dir, 0o500)
        rows, state, message, _, _, _ = query(
            "ALTER DATABASE " + database + " OWNER TO bob;")
        assert state == "58030", (rows, state, message)
        assert options.read_bytes() == original

        os.chmod(data_dir, original_mode)
        good("ALTER DATABASE " + database + " OWNER TO bob;")
        assert b"bob" in options.read_bytes()

        saved = data_dir / ".pg_database_options.saved"
        options.rename(saved)
        options.mkdir()
        marker = options / "block_rename"
        marker.write_text("occupied")
        try:
            rows, state, message, _, _, _ = query(
                "ALTER DATABASE " + database +
                " RENAME TO options_atomic_write_new;")
            assert state == "58030", (rows, state, message)
            assert (data_dir / database).is_dir()
            assert not (data_dir / "options_atomic_write_new").exists()
        finally:
            marker.unlink()
            options.rmdir()
            saved.rename(options)

        good("DROP DATABASE " + database + ";")
        print("[DATABASE OPTIONS ATOMIC WRITE PROTOCOL E2E] passed")
    finally:
        os.chmod(data_dir, original_mode)
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
