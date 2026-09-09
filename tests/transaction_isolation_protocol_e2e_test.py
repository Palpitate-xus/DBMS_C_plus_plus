#!/usr/bin/env python3
"""SET TRANSACTION rejects isolation changes after snapshot use."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "transaction_isolation_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        def execute(sql):
            return runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)

        rows, state, message, _, _, _ = execute(
            "CREATE TABLE isolation_guard (id INT);")
        assert state is None and rows == [], (state, message, rows)
        rows, state, message, _, _, _ = execute(
            "INSERT INTO isolation_guard VALUES (1);")
        assert state is None and rows == [], (state, message, rows)

        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute(
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM isolation_guard;")
        assert state is None and rows == [["1"]], (state, message, rows)

        _, state, message, _, _, _ = execute(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        assert state == "25001", (state, message)
        assert "before any query" in message, message
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)

        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = execute(
            "SELECT id FROM isolation_guard;")
        assert state is None and rows == [["1"]], (state, message, rows)
        _, state, message, _, _, _ = execute(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        assert state == "25001", (state, message)
        _, state, message, _, _, _ = execute(
            "/* not local recovery */ ROLLBACK PREPARED 'missing';")
        assert state == "25P02", (state, message)
        _, state, message, _, _, _ = execute(
            "ROLLBACK /* comment separates keywords */ PREPARED 'missing';")
        assert state == "25P02", (state, message)
        _, state, message, _, _, _ = execute(
            "-- leading recovery comment\n"
            "/* outer /* nested */ comment */ ROLLBACK;")
        assert state is None, (state, message)

        # Comment contents are trivia, not transaction-chain options.  A
        # failed transaction ended by this COMMIT must return to idle.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SELECT 1 / 0;")
        assert state == "22012", (state, message)
        commit_messages = client.simple_query(
            server["sock"], "COMMIT /* and chain */;")
        _, state, message, _, _, _ = runner.decode_wire_result(
            commit_messages, include_types=True)
        assert state is None, (state, message)
        ready = [payload for kind, payload in commit_messages if kind == b"Z"]
        assert ready == [b"I"], ready

        # Conversely, comments may separate real option tokens.  CHAIN must
        # start a replacement transaction after rolling the failed one back.
        _, state, message, _, _, _ = execute("BEGIN;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = execute("SELECT 1 / 0;")
        assert state == "22012", (state, message)
        chain_messages = client.simple_query(
            server["sock"], "COMMIT AND /* separator */ CHAIN;")
        _, state, message, _, _, _ = runner.decode_wire_result(
            chain_messages, include_types=True)
        assert state is None, (state, message)
        ready = [payload for kind, payload in chain_messages if kind == b"Z"]
        assert ready == [b"T"], ready
        _, state, message, _, _, _ = execute("ROLLBACK;")
        assert state is None, (state, message)
        print("[TRANSACTION ISOLATION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
