#!/usr/bin/env python3
"""A second FOR SHARE NOWAIT reader is compatible with the first."""

import importlib.util
from pathlib import Path
import socket


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "row_share_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    second = None

    def wire_query(sock, sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    def query(sock, sql):
        rows, state, message, _, _, _ = wire_query(sock, sql)
        assert state is None, (sql, state, message)
        return rows

    try:
        second = socket.create_connection(("127.0.0.1", server["port"]), timeout=15)
        second.settimeout(15)
        client.startup(second, "alice", "info")
        first = server["sock"]
        query(first, "CREATE TABLE row_share_nowait (id INT PRIMARY KEY);")
        query(first, "INSERT INTO row_share_nowait VALUES (1);")
        query(first, "BEGIN;")
        query(second, "BEGIN;")
        assert query(first, "SELECT id FROM row_share_nowait FOR SHARE;") == [["1"]]
        assert query(second, "SELECT id FROM row_share_nowait FOR SHARE NOWAIT;") == [["1"]]
        query(second, "ROLLBACK;")
        query(first, "ROLLBACK;")

        query(first, "BEGIN;")
        query(second, "BEGIN;")
        assert query(first, "SELECT id FROM row_share_nowait FOR UPDATE;") == [["1"]]
        _, state, message, _, _, _ = wire_query(
            second, "SELECT id FROM row_share_nowait FOR SHARE NOWAIT;")
        assert state == "55P03", (state, message)
        _, state, message, _, _, _ = wire_query(second, "SELECT 1;")
        assert state == "25P02", (state, message)
        query(second, "ROLLBACK;")
        query(first, "ROLLBACK;")

        # A standalone locking SELECT is an implicit transaction too. It
        # must check an already-held row lock before returning a row.
        query(first, "BEGIN;")
        assert query(first, "SELECT id FROM row_share_nowait FOR UPDATE;") == [["1"]]
        _, state, message, _, _, _ = wire_query(
            second, "SELECT id FROM row_share_nowait FOR SHARE NOWAIT;")
        assert state == "55P03", (state, message)
        assert query(second, "SELECT id FROM row_share_nowait FOR SHARE SKIP LOCKED;") == []
        query(first, "ROLLBACK;")
        assert query(second, "SELECT id FROM row_share_nowait FOR SHARE NOWAIT;") == [["1"]]
        query(first, "BEGIN;")
        assert query(first, "SELECT id FROM row_share_nowait FOR UPDATE NOWAIT;") == [["1"]]
        query(first, "ROLLBACK;")
        print("[FOR SHARE NOWAIT PROTOCOL E2E] passed")
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
