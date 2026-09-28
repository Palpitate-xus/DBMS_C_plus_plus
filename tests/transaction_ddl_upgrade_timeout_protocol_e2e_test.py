#!/usr/bin/env python3
"""An explicit transaction's DDL database-lock upgrade honors lock_timeout."""

import importlib.util
from pathlib import Path
import socket
import threading
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "transaction_ddl_upgrade_pgdiff",
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
        query(first, "CREATE TABLE ddl_upgrade_timeout (id INT PRIMARY KEY);")
        query(first, "INSERT INTO ddl_upgrade_timeout VALUES (1);")
        query(first, "SET lock_timeout = 50;")
        query(first, "BEGIN;")
        query(second, "BEGIN;")
        assert query(second, "SELECT 1;") == [["1"]]

        outcome = {}

        def alter():
            try:
                outcome["result"] = wire_query(
                    first, "ALTER TABLE ddl_upgrade_timeout ADD COLUMN extra INT;")
            except Exception as error:
                outcome["error"] = error

        worker = threading.Thread(target=alter)
        worker.start()
        time.sleep(0.2)
        query(second, "COMMIT;")
        worker.join(timeout=10)
        assert not worker.is_alive(), "DDL did not finish after blocker committed"
        assert "error" not in outcome, outcome
        rows, state, message, _, _, _ = outcome["result"]
        assert state == "55P03", (rows, state, message)
        rows, state, message, _, _, _ = wire_query(first, "SELECT 1;")
        assert state == "25P02", (rows, state, message)
        query(first, "ROLLBACK;")
        assert query(second, "SELECT id FROM ddl_upgrade_timeout;") == [["1"]]
        print("[TRANSACTION DDL UPGRADE TIMEOUT PROTOCOL E2E] passed")
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
