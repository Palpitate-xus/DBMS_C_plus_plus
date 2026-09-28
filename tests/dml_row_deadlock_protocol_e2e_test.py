#!/usr/bin/env python3
"""DELETE and UPDATE row-lock wait cycles report deadlock SQLSTATE."""

import importlib.util
from pathlib import Path
import socket
import threading
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "dml_deadlock_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query(first, "CREATE TABLE delete_row_deadlock (id INT PRIMARY KEY, v INT);")
        query(first, "INSERT INTO delete_row_deadlock VALUES (1, 1), (2, 2);")
        for action in (
            "UPDATE delete_row_deadlock SET v = 3 WHERE id = 1;",
            "DELETE FROM delete_row_deadlock WHERE id = 1;",
        ):
            query(first, "BEGIN;")
            query(second, "BEGIN;")
            query(first, "SET lock_timeout = 2000;")
            query(second, "SET lock_timeout = 2000;")
            assert query(first, "SELECT id FROM delete_row_deadlock WHERE id = 1 FOR UPDATE;") == [["1"]]
            assert query(second, "SELECT id FROM delete_row_deadlock WHERE id = 2 FOR UPDATE;") == [["2"]]

            first_result = []
            first_error = []

            def wait_for_second_row():
                try:
                    first_result.append(wire_query(
                        first, "SELECT id FROM delete_row_deadlock WHERE id = 2 FOR UPDATE;"))
                except Exception as exc:
                    first_error.append(exc)

            waiter = threading.Thread(target=wait_for_second_row, daemon=True)
            waiter.start()
            time.sleep(0.1)
            _, deadlock_state, deadlock_message, _, _, _ = wire_query(second, action)
            _, aborted_state, aborted_message, _, _, _ = wire_query(second, "SELECT 1;")
            query(second, "ROLLBACK;")
            waiter.join(timeout=5)
            assert not waiter.is_alive(), "first waiter did not resume after rollback"
            assert not first_error, first_error
            assert first_result and first_result[0][0] == [["2"]], first_result
            query(first, "ROLLBACK;")
            assert deadlock_state == "40P01", (action, deadlock_state, deadlock_message)
            assert aborted_state == "25P02", (aborted_state, aborted_message)
            assert query(second, "SELECT id, v FROM delete_row_deadlock ORDER BY id;") == [
                ["1", "1"], ["2", "2"]]
        print("[DML ROW DEADLOCK PROTOCOL E2E] passed")
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
