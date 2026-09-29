#!/usr/bin/env python3
"""Concurrent ALTER DATABASE updates must not overwrite unrelated entries."""

from concurrent.futures import ThreadPoolExecutor
import importlib.util
from pathlib import Path
import socket
from threading import Barrier


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "database_options_concurrent_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sockets = []
    names = [f"options_parallel_{index}" for index in range(12)]

    def query(sock, sql):
        rows, state, message, _, _, _ = runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)
        assert state is None, (sql, rows, state, message)

    try:
        for name in names:
            query(server["sock"], "CREATE DATABASE " + name + ";")
            sock = socket.create_connection(
                ("127.0.0.1", server["port"]), timeout=15)
            sock.settimeout(15)
            client.startup(sock, "alice", "info")
            sockets.append(sock)

        options = Path(server["dir"]) / ".pg_database_options"
        with ThreadPoolExecutor(max_workers=len(names)) as pool:
            for round_number in range(4):
                barrier = Barrier(len(names))

                def update(index):
                    barrier.wait(timeout=15)
                    query(sockets[index],
                          f"ALTER DATABASE {names[index]} "
                          f"SET test_option TO 'round_{round_number}_{index}';")

                list(pool.map(update, range(len(names))))
                contents = options.read_text()
                for index, name in enumerate(names):
                    matching = [line for line in contents.splitlines()
                                if line.startswith(name + "|")]
                    assert len(matching) == 1, (name, matching, contents)
                    expected = f"round_{round_number}_{index}"
                    assert expected in matching[0], (name, matching[0])

        print("[DATABASE OPTIONS CONCURRENT PROTOCOL E2E] passed")
    finally:
        for sock in sockets:
            sock.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
