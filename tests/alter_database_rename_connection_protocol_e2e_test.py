#!/usr/bin/env python3
"""Database rename must not move a connected database's live directory."""

import importlib.util
from pathlib import Path
import socket
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "alter_database_rename_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    second = None
    third = None
    old = "rename_connected_source"
    new = "rename_connected_target"

    def query(sock, sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    def good(sock, sql):
        rows, state, message, _, _, _ = query(sock, sql)
        assert state is None, (sql, rows, state, message)
        return rows

    try:
        first = server["sock"]
        good(first, "CREATE DATABASE " + old + ";")
        second = socket.create_connection(("127.0.0.1", server["port"]), timeout=15)
        second.settimeout(15)
        client.startup(second, "alice", old)
        good(second, "CREATE TABLE rename_data (id INT PRIMARY KEY);")
        good(second, "INSERT INTO rename_data VALUES (42);")

        for sock in (second, first):
            rows, state, message, _, _, _ = query(
                sock, "ALTER DATABASE " + old + " RENAME TO " + new + ";")
            assert state == "55006", (rows, state, message)
            assert (Path(server["dir"]) / old).is_dir()
            assert not (Path(server["dir"]) / new).exists()
        assert good(second, "SELECT id FROM rename_data;") == [["42"]]

        second.close()
        second = None
        deadline = time.monotonic() + 5
        while True:
            rows, state, message, _, _, _ = query(
                first, "ALTER DATABASE " + old + " RENAME TO " + new + ";")
            if state is None:
                break
            assert state == "55006", (rows, state, message)
            assert time.monotonic() < deadline, "backend did not disconnect"
            time.sleep(0.01)
        assert not (Path(server["dir"]) / old).exists()
        assert (Path(server["dir"]) / new).is_dir()
        third = socket.create_connection(("127.0.0.1", server["port"]), timeout=15)
        third.settimeout(15)
        client.startup(third, "alice", new)
        assert good(third, "SELECT id FROM rename_data;") == [["42"]]
        third.close()
        third = None
        deadline = time.monotonic() + 5
        while True:
            rows, state, message, _, _, _ = query(
                first, "DROP DATABASE " + new + ";")
            if state is None:
                break
            assert state == "55006", (rows, state, message)
            assert time.monotonic() < deadline, "renamed backend did not disconnect"
            time.sleep(0.01)
        print("[ALTER DATABASE RENAME CONNECTION PROTOCOL E2E] passed")
    finally:
        if third is not None:
            third.close()
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
