#!/usr/bin/env python3
"""DROP DATABASE must reject another connection using the target database."""

import importlib.util
from pathlib import Path
import socket
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "drop_database_idle_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    second = None
    database = "drop_database_idle_connection"

    def query(sock, sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    try:
        first = server["sock"]
        rows, state, message, _, _, _ = query(
            first, "CREATE DATABASE " + database + ";")
        assert state is None, (rows, state, message)

        second = socket.create_connection(("127.0.0.1", server["port"]), timeout=15)
        second.settimeout(15)
        client.startup(second, "alice", database)
        rows, state, message, _, _, _ = query(
            first, "DROP DATABASE " + database + ";")
        assert state == "55006", (rows, state, message)
        assert (Path(server["dir"]) / database).is_dir()
        rows, state, message, _, _, _ = query(second, "SELECT 1;")
        assert state is None and rows == [["1"]], (rows, state, message)

        second.close()
        second = None
        deadline = time.monotonic() + 5
        while True:
            rows, state, message, _, _, _ = query(
                first, "DROP DATABASE " + database + ";")
            if state is None:
                break
            assert state == "55006", (rows, state, message)
            assert time.monotonic() < deadline, "idle backend did not disconnect"
            time.sleep(0.01)
        assert not (Path(server["dir"]) / database).exists()
        print("[DROP DATABASE IDLE CONNECTION PROTOCOL E2E] passed")
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
