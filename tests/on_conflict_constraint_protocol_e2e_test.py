#!/usr/bin/env python3
"""Named ON CONFLICT arbiters, diagnostics, and wire recovery."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "on_conflict_constraint_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def execute(sql):
        messages = client.simple_query(server["sock"], sql)
        return runner.decode_wire_result(messages, include_types=True)

    try:
        setup = [
            ("CREATE TABLE named_conflict_wire ("
             "id INT, tenant INT, code TEXT, payload TEXT, "
             "CONSTRAINT named_conflict_wire_pkey PRIMARY KEY (id), "
             "CONSTRAINT named_conflict_wire_code_key UNIQUE (tenant, code), "
             "CONSTRAINT named_conflict_wire_check CHECK (tenant > 0));"),
            ("INSERT INTO named_conflict_wire "
             "VALUES (1, 10, 'alpha', 'old');"),
        ]
        for sql in setup:
            rows, state, message, _, _, _ = execute(sql)
            assert state is None and rows == [], (sql, state, message, rows)

        rows, state, message, headers, tag, type_oids = execute(
            "INSERT INTO named_conflict_wire "
            "VALUES (2, 10, 'alpha', 'new') "
            "ON CONFLICT ON CONSTRAINT named_conflict_wire_code_key "
            "DO UPDATE SET payload = excluded.payload RETURNING id, payload;")
        assert state is None, message
        assert rows == [["1", "new"]], rows
        assert headers == ["id", "payload"], headers
        assert tag == "INSERT 0 1" and type_oids == [23, 25], (tag, type_oids)

        rows, state, message, _, _, _ = execute(
            "INSERT INTO named_conflict_wire "
            "VALUES (3, 30, 'gamma', 'bad') "
            "ON CONFLICT ON CONSTRAINT missing_constraint DO NOTHING;")
        assert rows == [] and state == "42704", (state, message, rows)

        rows, state, message, _, _, _ = execute(
            "INSERT INTO named_conflict_wire "
            "VALUES (4, 40, 'delta', 'bad') "
            "ON CONFLICT ON CONSTRAINT named_conflict_wire_check DO NOTHING;")
        assert rows == [] and state == "42809", (state, message, rows)

        rows, state, message, _, tag, type_oids = execute(
            "SELECT id, payload FROM named_conflict_wire ORDER BY id;")
        assert state is None, message
        assert rows == [["1", "new"]], rows
        assert tag == "SELECT 1" and type_oids == [23, 25], (tag, type_oids)
        print("[ON CONFLICT CONSTRAINT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
