#!/usr/bin/env python3
"""Data-modifying CTEs preserve exact RETURNING rows and types."""

import importlib.util
from pathlib import Path


def decode(runner, client, server, sql):
    return runner.decode_wire_result(
        client.simple_query(server["sock"], sql), include_types=True)


def check_command_ctes(runner, client, server):
    def query(sql, kinds, tag=None, rows=(), state=None, ready=b"I", cte=None):
        frames = client.simple_query(server["sock"], sql)
        values, code, message, _, actual = runner.decode_wire_result(frames)
        assert (b"".join(kind for kind, _ in frames), actual, values, code, frames[-1]) == (
            kinds, tag, [list(row) for row in rows], state, (b"Z", ready)), (sql, frames)
        if cte is not None:
            assert message == f'WITH query "{cte}" does not have a RETURNING clause', message

    table = "public.cte_command_rows"
    aux = "public.cte_command_aux"
    seq = "public.cte_command_seq"
    sequence_value = f"nextval('{seq}')"
    contents = f"SELECT id,n FROM {table} ORDER BY id"
    initial = (("1", "10"), ("2", "20"), ("3", "30"))
    query(f"CREATE TABLE {table}(id integer PRIMARY KEY,n integer)", b"CZ", "CREATE TABLE")
    query(f"CREATE TABLE {aux}(id integer PRIMARY KEY,n integer)", b"CZ", "CREATE TABLE")
    query(f"CREATE SEQUENCE {seq} START WITH 50", b"CZ", "CREATE SEQUENCE")
    query(f"INSERT INTO {table} VALUES (1,10),(2,20),(3,30)", b"CZ", "INSERT 0 3")
    query(f"INSERT INTO {aux} VALUES (1,100)", b"CZ", "INSERT 0 1")
    update = f"UPDATE {table} SET n={sequence_value} WHERE id=1"
    delete = f"DELETE FROM {table} WHERE id=2"
    insert = f"INSERT INTO {table} VALUES (4,{sequence_value})"
    for sql in (
        f"WITH changed AS ({insert}) SELECT * FROM changed",
        f"WITH changed AS ({update}) SELECT FROM changed",
        f"WITH changed AS ({delete}) SELECT count(*) FROM changed LIMIT 0",
        f"WITH changed AS ({update}) SELECT count(*) FROM (SELECT * FROM changed) q",
        f"WITH changed AS ({update}) SELECT EXISTS(SELECT FROM changed)",
        f"WITH changed AS ({update}) SELECT 1 UNION SELECT count(*) FROM changed",
        f"WITH prev AS ({update} RETURNING id), changed AS ({delete}), "
        "later AS (SELECT count(*) FROM changed) SELECT * FROM later",
        f"WITH changed AS ({delete}) INSERT INTO {aux} SELECT * FROM changed",
        f"WITH changed AS ({delete}) UPDATE {aux} SET n=0 FROM changed",
        f"WITH changed AS ({delete}) DELETE FROM {aux} USING changed",
    ):
        query(sql, b"EZ", state="0A000", cte="changed")
        query(contents, b"TDDDCZ", "SELECT 3", initial)
    query(f'WITH "No result" AS ({update}) SELECT * FROM "No result"',
          b"EZ", state="0A000", cte="No result")
    query(f"WITH changed AS ({update}) SELECT 1 FROM changed ORDER BY",
          b"EZ", state="42601")
    query(f"INSERT INTO {aux} VALUES (2,200); "
          f"WITH changed AS ({update}) SELECT * FROM changed; "
          f"INSERT INTO {aux} VALUES (3,300)",
          b"CEZ", "INSERT 0 1", state="0A000", cte="changed")
    query(f"SELECT {sequence_value}", b"TDCZ", "SELECT 1", (("50",),))
    query(f"SELECT id,n FROM {aux}", b"TDCZ", "SELECT 1", (("1", "100"),))

    query("BEGIN", b"CZ", "BEGIN", ready=b"T")
    query(f"UPDATE {aux} SET n=101 WHERE id=1", b"CZ", "UPDATE 1", ready=b"T")
    query("SAVEPOINT no_result", b"CZ", "SAVEPOINT", ready=b"T")
    query(f"WITH changed AS ({update}) SELECT * FROM changed", b"EZ",
          state="0A000", ready=b"E", cte="changed")
    query(contents, b"EZ", state="25P02", ready=b"E")
    query("ROLLBACK TO SAVEPOINT no_result", b"CZ", "ROLLBACK", ready=b"T")
    query(contents, b"TDDDCZ", "SELECT 3", initial, ready=b"T")
    query(f"SELECT id,n FROM {aux}", b"TDCZ", "SELECT 1", (("1", "101"),), ready=b"T")
    query("COMMIT", b"CZ", "COMMIT")

    query(f"WITH changed AS ({update}) SELECT 1 LIMIT 0", b"TCZ", "SELECT 0")
    query(f"WITH changed AS ({delete}) "
          f"SELECT * FROM (WITH changed AS (SELECT id,n FROM {aux}) SELECT * FROM changed) q",
          b"TDCZ", "SELECT 1", (("1", "101"),))
    query(f"WITH changed AS (UPDATE {table} SET n=100 WHERE id=3 RETURNING n) "
          "SELECT n FROM changed", b"TDCZ", "SELECT 1", (("100",),))
    query(contents, b"TDDCZ", "SELECT 2", (("1", "51"), ("3", "100")))
    query(f'WITH "__cte_0" AS (UPDATE {table} SET n=52 WHERE id=1), '
          "read_back AS (SELECT 123 AS v) SELECT v FROM read_back",
          b"TDCZ", "SELECT 1", (("123",),))
    query(f"WITH changed AS (UPDATE {table} SET n=53 WHERE id=1) "
          f"SELECT b.id,changed.n FROM {table} b "
          "CROSS JOIN LATERAL (SELECT b.n AS n) changed ORDER BY b.id",
          b"TDDCZ", "SELECT 2", (("1", "52"), ("3", "100")))
    query(f"WITH changed AS (UPDATE {table} SET n=54 WHERE id=1) "
          f"SELECT b.id,q.n AS changed FROM {table} b "
          "CROSS JOIN LATERAL (SELECT b.n AS n) q ORDER BY b.id",
          b"TDDCZ", "SELECT 2", (("1", "53"), ("3", "100")))
    query(contents, b"TDDCZ", "SELECT 2", (("1", "54"), ("3", "100")))


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
        check_command_ctes(runner, client, server)
        print("[DML CTE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
