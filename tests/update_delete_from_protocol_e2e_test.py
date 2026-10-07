#!/usr/bin/env python3
"""Source-driven UPDATE/DELETE aliases, cardinality, and atomic failures."""

import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "source_dml_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    def query(sql):
        messages = client.simple_query(sock, sql)
        statuses = [body for kind, body in messages if kind == b"Z"]
        assert statuses == [b"I"], (sql, statuses)
        decoded = runner.decode_wire_result(messages, include_types=True)
        print("SOURCE_DML", sql, decoded, "READY", statuses, flush=True)
        return decoded

    try:
        setup = [
            "CREATE TABLE source_dml_target "
            "(id INT PRIMARY KEY, val INT UNIQUE);",
            "CREATE TABLE source_dml_source (id INT, val INT);",
            "INSERT INTO source_dml_target VALUES (1, 10), (2, 20), (3, 30);",
            "INSERT INTO source_dml_source VALUES "
            "(1, 110), (1, 111), (2, 220);",
        ]
        for sql in setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)

        # A FROM/USING keyword with no valid source must never disappear
        # from the AST and turn a malformed statement into target-only DML.
        for malformed_sql in (
                "UPDATE source_dml_target SET val = id + 100 FROM;",
                "UPDATE source_dml_target SET val = id + 100 "
                "FROM source_dml_source JOIN;",
                "UPDATE source_dml_target dst SET val = dst.val + 100 "
                "FROM source_dml_source a JOIN source_dml_source b;",
                "DELETE FROM source_dml_target USING;",
                "DELETE FROM source_dml_target "
                "USING source_dml_source LEFT JOIN;",
                "DELETE FROM source_dml_target "
                "USING source_dml_source a JOIN source_dml_source b;",
                "DELETE FROM source_dml_target USING source_dml_source a "
                "JOIN source_dml_source b CROSS JOIN source_dml_source c;"):
            _, state, message, _, command_tag, _ = query(malformed_sql)
            assert state == "42601", (malformed_sql, state, message)
            assert command_tag is None, (malformed_sql, command_tag)
            rows, state, message, headers, command_tag, type_oids = query(
                "SELECT id, val FROM source_dml_target ORDER BY id;")
            assert state is None, (state, message)
            assert rows == [["1", "10"], ["2", "20"], ["3", "30"]], rows
            assert headers == ["id", "val"] and type_oids == [23, 23], (
                headers, type_oids)
            assert command_tag == "SELECT 3", command_tag

        rows, state, message, headers, command_tag, type_oids = query(
            "UPDATE source_dml_target AS dst SET val = src.val "
            "FROM source_dml_source AS src WHERE dst.id = src.id "
            "RETURNING id, val;")
        assert state == "42702" and command_tag is None, (state, message, command_tag)
        rows, state, message, headers, command_tag, type_oids = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None and rows == [["1", "10"], ["2", "20"], ["3", "30"]], (rows, state)
        assert headers == ["id", "val"] and type_oids == [23, 23], (headers, type_oids)
        rows, state, message, headers, command_tag, type_oids = query(
            "UPDATE source_dml_target AS dst SET val = src.val "
            "FROM source_dml_source AS src WHERE dst.id = src.id "
            "RETURNING dst.id, dst.val;")
        assert state is None, (state, message)
        assert command_tag == "UPDATE 2", command_tag
        assert headers == ["id", "val"] and type_oids == [23, 23], (
            headers, type_oids)
        assert len(rows) == 2, rows
        values = {row[0]: row[1] for row in rows}
        assert values["1"] in {"110", "111"}, values
        assert values["2"] == "220", values

        rows, state, message, _, command_tag, _ = query(
            "DELETE FROM source_dml_target AS dst "
            "USING source_dml_source AS src "
            "WHERE dst.id = src.id AND dst.id = 2 RETURNING id, val;")
        assert state == "42702" and command_tag is None, (state, message, command_tag)
        unchanged, state, message, headers, _, type_oids = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None and unchanged == [["1", values["1"]], ["2", "220"], ["3", "30"]], (unchanged, state)
        assert headers == ["id", "val"] and type_oids == [23, 23], (headers, type_oids)
        rows, state, message, _, command_tag, _ = query(
            "DELETE FROM source_dml_target AS dst "
            "USING source_dml_source AS src "
            "WHERE dst.id = src.id AND dst.id = 2 RETURNING dst.id, dst.val;")
        assert state is None, (state, message)
        assert rows == [["2", "220"]], rows
        assert command_tag == "DELETE 1", command_tag

        # Valid SQL outside the bounded INNER/CROSS executor fails closed.
        _, state, _, _, _, _ = query(
            "UPDATE source_dml_target AS dst SET val = src.val "
            "FROM source_dml_source AS src LEFT JOIN source_dml_source AS extra "
            "ON src.id = extra.id WHERE dst.id = src.id;")
        # The old bounded UPDATE source executor still rejects outer joins.
        # PostgreSQL executes this legal source; this is an explicit OPEN
        # capability boundary, not a claim that 0A000 matches PostgreSQL.
        assert state == (None if reference else "0A000"), state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None, (state, message)
        assert len(rows) == 2 and rows[-1] == ["3", "30"], rows

        # Cursor-positioned mutations are recognized, rejected explicitly,
        # and leave both the target and the connection usable.
        _, state, _, _, _, _ = query(
            "UPDATE source_dml_target SET val = 999 "
            "WHERE CURRENT OF missing_cursor;")
        # Missing named cursors have 34000 in PostgreSQL. The current project
        # cursor-positioned mutation boundary is still explicitly unsupported.
        assert state == ("34000" if reference else "0A000"), state
        _, state, _, _, _, _ = query(
            "DELETE FROM source_dml_target WHERE CURRENT OF missing_cursor;")
        assert state == ("34000" if reference else "0A000"), state
        rows, state, message, _, command_tag, _ = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None, (state, message)
        assert len(rows) == 2 and command_tag == "SELECT 2", (rows, command_tag)

        atomic_setup = [
            "CREATE TABLE source_atomic_target "
            "(id INT PRIMARY KEY, val INT UNIQUE);",
            "CREATE TABLE source_atomic_source (id INT, val INT);",
            "INSERT INTO source_atomic_target VALUES "
            "(1, 10), (2, 20), (3, 40);",
            "INSERT INTO source_atomic_source VALUES (1, 30), (2, 40);",
        ]
        for sql in atomic_setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)
        _, state, _, _, _, _ = query(
            "UPDATE source_atomic_target AS dst SET val = src.val "
            "FROM source_atomic_source AS src WHERE dst.id = src.id;")
        assert state == "23505", state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM source_atomic_target ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "10"], ["2", "20"], ["3", "40"]], rows

        # A final successful statement proves protocol recovery after errors.
        _, state, message, _, command_tag, _ = query(
            "UPDATE source_atomic_target SET val = 50 WHERE id = 1;")
        assert state is None, (state, message)
        assert command_tag == "UPDATE 1", command_tag
        print("[UPDATE/DELETE FROM PROTOCOL E2E] passed")
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                for table in ("source_dml_target", "source_dml_source", "source_atomic_target", "source_atomic_source"):
                    client.simple_query(sock, "DROP TABLE IF EXISTS " + table + ";")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
