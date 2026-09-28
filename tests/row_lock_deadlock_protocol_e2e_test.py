#!/usr/bin/env python3
"""A row-lock wait cycle must report deadlock, not a lock timeout."""

import importlib.util
from pathlib import Path
import socket
import threading
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "row_lock_deadlock_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query(first, "CREATE TABLE row_lock_deadlock (id INT PRIMARY KEY);")
        query(first, "INSERT INTO row_lock_deadlock VALUES (1), (2);")
        query(first, "BEGIN;")
        query(second, "BEGIN;")
        query(first, "SET lock_timeout = 2000;")
        query(second, "SET lock_timeout = 2000;")
        assert query(first, "SELECT id FROM row_lock_deadlock WHERE id = 1 FOR UPDATE;") == [["1"]]
        assert query(second, "SELECT id FROM row_lock_deadlock WHERE id = 2 FOR UPDATE;") == [["2"]]

        first_result = []
        first_error = []

        def wait_for_second_row():
            try:
                first_result.append(wire_query(
                    first, "SELECT id FROM row_lock_deadlock WHERE id = 2 FOR UPDATE;"))
            except Exception as exc:
                first_error.append(exc)

        waiter = threading.Thread(target=wait_for_second_row, daemon=True)
        waiter.start()
        time.sleep(0.1)
        _, state, message, _, _, _ = wire_query(
            second, "SELECT id FROM row_lock_deadlock WHERE id = 1 FOR UPDATE;")
        deadlock_state, deadlock_message = state, message
        _, aborted_state, aborted_message, _, _, _ = wire_query(second, "SELECT 1;")
        query(second, "ROLLBACK;")
        waiter.join(timeout=5)
        assert not waiter.is_alive(), "first waiter did not resume after rollback"
        assert not first_error, first_error
        assert first_result and first_result[0][0] == [["2"]], first_result
        query(first, "ROLLBACK;")
        assert deadlock_state == "40P01", (deadlock_state, deadlock_message)
        assert aborted_state == "25P02", (aborted_state, aborted_message)
        print("[ROW LOCK DEADLOCK PROTOCOL E2E] passed")
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
