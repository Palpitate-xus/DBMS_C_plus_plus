#!/usr/bin/env python3
"""MERGE branches, RETURNING, fail-closed sources, and atomic rollback."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "merge_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    try:
        setup = [
            "CREATE TABLE merge_wire_target (id INT PRIMARY KEY, val INT UNIQUE);",
            "CREATE TABLE merge_wire_source (id INT, val INT);",
            "INSERT INTO merge_wire_target VALUES (1, 10), (2, 20), (3, 30);",
            "INSERT INTO merge_wire_source VALUES (1, 110), (2, 220), "
            "(4, 440), (5, -1);",
        ]
        for sql in setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)

        rows, state, message, headers, command_tag, type_oids = query(
            "MERGE INTO merge_wire_target AS dst "
            "USING merge_wire_source AS src ON dst.id = src.id "
            "WHEN MATCHED AND src.val > 200 THEN DELETE "
            "WHEN MATCHED AND src.val < 0 THEN DO NOTHING "
            "WHEN MATCHED THEN UPDATE SET val = src.val "
            "WHEN NOT MATCHED BY TARGET AND src.val < 0 THEN DO NOTHING "
            "WHEN NOT MATCHED BY TARGET THEN INSERT (id, val) "
            "VALUES (src.id, src.val) "
            "WHEN NOT MATCHED BY SOURCE THEN DELETE "
            "RETURNING id, val;")
        assert state is None, (state, message)
        assert sorted(rows) == sorted([
            ["1", "110"], ["2", "20"], ["3", "30"], ["4", "440"]
        ]), rows
        assert headers == ["id", "val"], headers
        assert type_oids == [23, 23], type_oids
        assert command_tag == "MERGE 4", command_tag

        rows, state, message, _, command_tag, _ = query(
            "SELECT id, val FROM merge_wire_target ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "110"], ["4", "440"]], rows
        assert command_tag == "SELECT 2", command_tag

        rows, state, message, headers, command_tag, type_oids = query(
            "MERGE INTO merge_wire_target AS dst "
            "USING merge_wire_source AS src ON dst.id = src.id "
            "WHEN MATCHED THEN DO NOTHING;")
        assert state is None, (state, message)
        assert rows == [] and headers == [] and type_oids == [], (
            rows, headers, type_oids)
        assert command_tag == "MERGE 0", command_tag

        null_setup = [
            "CREATE TABLE merge_text_target (id INT PRIMARY KEY, val TEXT);",
            "CREATE TABLE merge_text_source (id INT, val TEXT);",
            "INSERT INTO merge_text_source VALUES "
            "(1, ''), (2, NULL), (3, 'NULL');",
        ]
        for sql in null_setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)
        rows, state, message, headers, command_tag, type_oids = query(
            "MERGE INTO merge_text_target AS dst "
            "USING merge_text_source AS src ON dst.id = src.id "
            "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val) "
            "RETURNING id, val;")
        assert state is None, (state, message)
        assert rows == [["1", ""], ["2", None], ["3", "NULL"]], rows
        assert headers == ["id", "val"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "MERGE 3", command_tag

        atomic_setup = [
            "CREATE TABLE merge_atomic_target "
            "(id INT PRIMARY KEY, val INT UNIQUE);",
            "CREATE TABLE merge_atomic_source (id INT, val INT);",
            "INSERT INTO merge_atomic_target VALUES (1, 10), (2, 20);",
            "INSERT INTO merge_atomic_source VALUES (1, 30), (3, 20);",
        ]
        for sql in atomic_setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)
        _, state, _, _, _, _ = query(
            "MERGE INTO merge_atomic_target AS dst "
            "USING merge_atomic_source AS src ON dst.id = src.id "
            "WHEN MATCHED THEN UPDATE SET val = src.val "
            "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val);")
        assert state == "23505", state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM merge_atomic_target ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "10"], ["2", "20"]], rows

        cardinality_setup = [
            "CREATE TABLE merge_card_target (id INT PRIMARY KEY, val INT);",
            "CREATE TABLE merge_card_source (id INT, val INT);",
            "INSERT INTO merge_card_target VALUES (1, 10);",
            "INSERT INTO merge_card_source VALUES (1, 11), (1, 12);",
        ]
        for sql in cardinality_setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)
        _, state, _, _, _, _ = query(
            "MERGE INTO merge_card_target AS dst "
            "USING merge_card_source AS src ON dst.id = src.id "
            "WHEN MATCHED THEN UPDATE SET val = src.val;")
        assert state == "21000", state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM merge_card_target;")
        assert state is None, (state, message)
        assert rows == [["1", "10"]], rows

        # Outer source joins are valid SQL but outside the bounded executor.
        # They must return 0A000 before touching the target.
        outer_setup = [
            "CREATE TABLE merge_outer_target (id INT PRIMARY KEY, val INT);",
            "CREATE TABLE merge_outer_a (id INT, grp INT);",
            "CREATE TABLE merge_outer_b (grp INT, val INT);",
            "INSERT INTO merge_outer_target VALUES (1, 10);",
            "INSERT INTO merge_outer_a VALUES (1, 7);",
        ]
        for sql in outer_setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)
        _, state, _, _, _, _ = query(
            "MERGE INTO merge_outer_target AS dst "
            "USING merge_outer_a AS a LEFT JOIN merge_outer_b AS b "
            "ON a.grp = b.grp ON dst.id = a.id "
            "WHEN MATCHED THEN DELETE;")
        assert state == "0A000", state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM merge_outer_target;")
        assert state is None, (state, message)
        assert rows == [["1", "10"]], rows

        print("[MERGE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
