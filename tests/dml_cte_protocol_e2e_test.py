#!/usr/bin/env python3
"""Data-modifying CTEs preserve exact RETURNING rows and types."""

import importlib.util
from pathlib import Path


def decode(runner, client, server, sql):
    return runner.decode_wire_result(
        client.simple_query(server["sock"], sql), include_types=True)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "dml_cte_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        rows, state, message, _, _, _ = decode(
            runner, client, server,
            "CREATE TABLE dml_cte_rows (id INT, txt TEXT);")
        assert state is None and rows == [], (state, message, rows)

        cases = [
            (("WITH inserted AS ("
              "INSERT INTO dml_cte_rows VALUES "
              "(1, 'a b'), (2, ''), (3, 'NULL'), (4, NULL) "
              "RETURNING id, txt) "
              "SELECT id, txt FROM inserted ORDER BY id;"),
             [["1", "a b"], ["2", ""], ["3", "NULL"], ["4", None]],
             ["id", "txt"], [23, 25]),
            (("WITH updated(row_id, body) AS ("
              "UPDATE dml_cte_rows SET txt = 'c d' WHERE id = 1 "
              "RETURNING id, txt) "
              "SELECT row_id, body FROM updated;"),
             [["1", "c d"]], ["row_id", "body"], [23, 25]),
            (("WITH deleted AS ("
              "DELETE FROM dml_cte_rows WHERE id = 4 RETURNING id, txt) "
              "SELECT id, txt FROM deleted;"),
             [["4", None]], ["id", "txt"], [23, 25]),
            (("WITH changed AS ("
              "UPDATE dml_cte_rows SET txt = 'changed' WHERE id = 3) "
              "SELECT 1 AS answer;"),
             [["1"]], ["answer"], [23]),
        ]
        for sql, expected_rows, expected_headers, expected_types in cases:
            decoded = decode(runner, client, server, sql)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows, expected_rows)
            assert headers == expected_headers, (sql, headers, expected_headers)
            assert type_oids == expected_types, (sql, type_oids, expected_types)
            assert command_tag == "SELECT %d" % len(expected_rows), (
                sql, command_tag)

        decoded = decode(
            runner, client, server,
            "SELECT id, txt FROM dml_cte_rows ORDER BY id;")
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [["1", "c d"], ["2", ""], ["3", "changed"]], rows
        assert headers == ["id", "txt"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 3", command_tag

        rows, state, message, _, _, _ = decode(
            runner, client, server,
            "CREATE TABLE dml_cte_snapshot_rows (id INT, txt TEXT);")
        assert state is None and rows == [], (state, message, rows)
        rows, state, message, _, _, _ = decode(
            runner, client, server,
            "INSERT INTO dml_cte_snapshot_rows VALUES (1, 'old');")
        assert state is None and rows == [], (state, message, rows)

        snapshot_cases = [
            (("WITH ins AS (INSERT INTO dml_cte_snapshot_rows "
              "VALUES (2, 'new') RETURNING id) "
              "SELECT count(*) AS seen FROM dml_cte_snapshot_rows;"),
             [["1"]], ["seen"], [20]),
            (("WITH upd AS (UPDATE dml_cte_snapshot_rows SET txt = 'updated' "
              "WHERE id = 1 RETURNING id) "
              "SELECT txt FROM dml_cte_snapshot_rows WHERE id = 1;"),
             [["old"]], ["txt"], [25]),
            (("WITH del AS (DELETE FROM dml_cte_snapshot_rows "
              "WHERE id = 2 RETURNING id) "
              "SELECT count(*) AS seen FROM dml_cte_snapshot_rows;"),
             [["2"]], ["seen"], [20]),
        ]
        for sql, expected_rows, expected_headers, expected_types in snapshot_cases:
            rows, state, message, headers, command_tag, type_oids = decode(
                runner, client, server, sql)
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows, expected_rows)
            assert headers == expected_headers, (sql, headers)
            assert type_oids == expected_types, (sql, type_oids)
            assert command_tag == "SELECT 1", (sql, command_tag)

        for sql in ("BEGIN;",
                    "INSERT INTO dml_cte_snapshot_rows VALUES (3, 'prior');"):
            rows, state, message, _, _, _ = decode(
                runner, client, server, sql)
            assert state is None and rows == [], (sql, state, message, rows)
        rows, state, message, headers, command_tag, type_oids = decode(
            runner, client, server,
            "WITH upd AS (UPDATE dml_cte_snapshot_rows SET txt = 'after' "
            "WHERE id = 3 RETURNING id) "
            "SELECT txt FROM dml_cte_snapshot_rows WHERE id = 3;")
        assert state is None, (state, message)
        assert rows == [["prior"]], rows
        assert headers == ["txt"] and type_oids == [25], (headers, type_oids)
        assert command_tag == "SELECT 1", command_tag
        rows, state, message, headers, command_tag, type_oids = decode(
            runner, client, server,
            "SELECT txt FROM dml_cte_snapshot_rows WHERE id = 3;")
        assert state is None and rows == [["after"]], (state, message, rows)
        assert headers == ["txt"] and type_oids == [25], (headers, type_oids)
        assert command_tag == "SELECT 1", command_tag
        rows, state, message, _, _, _ = decode(
            runner, client, server, "ROLLBACK;")
        assert state is None and rows == [], (state, message, rows)

        rows, state, message, headers, command_tag, type_oids = decode(
            runner, client, server,
            "SELECT id, txt FROM dml_cte_snapshot_rows ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "updated"]], rows
        assert headers == ["id", "txt"] and type_oids == [23, 25], (
            headers, type_oids)
        assert command_tag == "SELECT 1", command_tag
        print("[DML CTE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
