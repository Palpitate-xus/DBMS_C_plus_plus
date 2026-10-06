#!/usr/bin/env python3
"""Implicit read owners retain real index access without losing old snapshots."""
import importlib.util
from pathlib import Path
import socket


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("read_owner_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    other = None

    def query(sql, rows=None, ready=b"I", sock=None):
        messages = client.simple_query(sock or server["sock"], sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] is None and messages[-1] == (b"Z", ready), (sql, result, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        return result

    def stats(ready=b"I"):
        result = query("SELECT * FROM pg_catalog.pg_stat_tables;", ready=ready)
        row = next(row for row in result[0] if row[0] == "read_owner_rows")
        return [int(value) for value in row[1:]]

    try:
        query("CREATE TABLE read_owner_rows(id INT);")
        query("INSERT INTO read_owner_rows VALUES(1),(2);")
        query("CREATE INDEX read_owner_id ON read_owner_rows(id);")
        before = stats()
        query("SELECT id FROM read_owner_rows WHERE id=1;", [["1"]])
        after = stats()
        assert after[2] == before[2] + 1 and after[3] == before[3] + 1, (before, after)
        other = socket.create_connection(("127.0.0.1", server["port"]), timeout=runner.wire_timeout())
        client.startup(other, "alice", "info")
        query("BEGIN ISOLATION LEVEL REPEATABLE READ;", ready=b"T")
        before = stats(b"T")
        query("SELECT id FROM read_owner_rows WHERE id=1;", [["1"]], b"T")
        after = stats(b"T")
        assert after[2] == before[2] + 1 and after[3] == before[3] + 1, (before, after)
        query("UPDATE read_owner_rows SET id=20 WHERE id=2;", sock=other)
        before = stats(b"T")
        query("SELECT id FROM read_owner_rows WHERE id=2;", [["2"]], b"T")
        after = stats(b"T")
        assert after[2] == before[2] and after[0] == before[0] + 1, (before, after)
        query("SELECT id FROM read_owner_rows WHERE id=20;", [], b"T")
        query("COMMIT;")
        query("SELECT id FROM read_owner_rows WHERE id=20;", [["20"]])

        query("BEGIN;", sock=other, ready=b"T")
        query("BEGIN;", ready=b"T")
        before = stats(b"T")
        query("SELECT id FROM read_owner_rows WHERE id=20;", [["20"]], b"T")
        after = stats(b"T")
        assert after[2] == before[2] and after[0] == before[0] + 1, (before, after)
        query("COMMIT;")
        query("ROLLBACK;", sock=other)

        query("BEGIN;", ready=b"T")
        query("UPDATE read_owner_rows SET id=30 WHERE id=20;", ready=b"T")
        before = stats(b"T")
        query("SELECT id FROM read_owner_rows WHERE id=30;", [["30"]], b"T")
        after = stats(b"T")
        assert after[2] == before[2] and after[0] == before[0] + 1, (before, after)
        query("ROLLBACK;")
        query("SELECT id FROM read_owner_rows WHERE id=20;", [["20"]])
        print("[READ OWNER INDEX SNAPSHOT PROTOCOL E2E] passed")
    finally:
        if other is not None:
            other.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
