#!/usr/bin/env python3
"""A transaction's SELECT keeps its relation lock until transaction end."""

import importlib.util
from pathlib import Path
import socket


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "transaction_select_lock_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
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
        query(first, "CREATE TABLE transaction_read_lock (id INT PRIMARY KEY);")
        query(first, "INSERT INTO transaction_read_lock VALUES (1);")
        query(second, "SET lock_timeout = 50;")
        query(first, "BEGIN;")
        assert query(first, "SELECT id FROM transaction_read_lock;") == [["1"]]

        rows, state, message, _, _, _ = wire_query(
            second, "ALTER TABLE transaction_read_lock ADD COLUMN extra INT;")
        assert state == "55P03", (rows, state, message)
        assert query(second, "SELECT 1;") == [["1"]]

        query(first, "COMMIT;")
        query(second, "ALTER TABLE transaction_read_lock ADD COLUMN extra INT;")
        assert query(second, "SELECT id FROM transaction_read_lock;") == [["1"]]
        print("[TRANSACTION SELECT TABLE LOCK PROTOCOL E2E] passed")
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
