#!/usr/bin/env python3
"""Rolled-back sequence identities cannot inherit another sequence's currval."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "sequence_identity_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def error(sql, expected):
        _, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected, (sql, state, message)

    try:
        query("CREATE SEQUENCE identity_survivor START 41;")
        assert query("SELECT nextval('identity_survivor');") == [["41"]]
        query("BEGIN;")
        query("CREATE SEQUENCE identity_rollback;")
        assert query("SELECT nextval('identity_rollback');") == [["1"]]
        query("ROLLBACK;")
        error("SELECT nextval('identity_rollback');", "42P01")
        assert query("SELECT currval('identity_survivor');") == [["41"]]
        query("CREATE SEQUENCE identity_rollback;")
        error("SELECT currval('identity_rollback');", "55000")
        assert query("SELECT nextval('identity_rollback');") == [["1"]]
        query("BEGIN;")
        query("SAVEPOINT identity_sp;")
        query("CREATE SEQUENCE identity_savepoint;")
        assert query("SELECT nextval('identity_savepoint');") == [["1"]]
        query("ROLLBACK TO SAVEPOINT identity_sp;")
        error("SELECT nextval('identity_savepoint');", "42P01")
        query("ROLLBACK TO SAVEPOINT identity_sp;")
        query("CREATE SEQUENCE identity_savepoint;")
        error("SELECT currval('identity_savepoint');", "55000")
        query("ROLLBACK TO SAVEPOINT identity_sp;")
        query("COMMIT;")
        assert query("SELECT currval('identity_survivor');") == [["41"]]
        print("[SEQUENCE ROLLBACK IDENTITY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
