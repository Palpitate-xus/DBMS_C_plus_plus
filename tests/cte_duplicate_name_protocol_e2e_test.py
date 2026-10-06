#!/usr/bin/env python3
"""Duplicate WITH names must be rejected before any CTE or outer write."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("duplicate_cte_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]

    def query(sql):
        messages = client.simple_query(sock, sql)
        return runner.decode_wire_result(messages, include_types=True), messages

    def invalid(sql, parse_only, ready, state="42712"):
        if parse_only:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") + client.typed(b"H", b""))
            kind, payload = client.read_message(sock)
            assert kind == b"E", (sql, kind, payload)
            assert client.diagnostic_fields(payload).get(b"C") == state.encode(), (sql, payload)
            sock.sendall(client.typed(b"S", b""))
            assert client.read_until_ready(sock) == [(b"Z", ready)], sql
        else:
            result, messages = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
            assert not any(kind in (b"C", b"D") for kind, _ in messages), (sql, messages)
            assert messages[-1] == (b"Z", ready), (sql, messages[-1])

    bad = (
        "WITH c AS(SELECT 1 AS id),c AS(SELECT 2 AS id) SELECT id FROM c;",
        'WITH C AS(SELECT 1),"c" AS(SELECT 2) SELECT 1;',
        'WITH "a""b" AS(SELECT 1),"a""b" AS(SELECT 2) SELECT 1;',
        "WITH RECURSIVE c AS(SELECT 1),c AS(SELECT 2) SELECT 1;",
        "WITH c(id) AS MATERIALIZED(SELECT 1),c(id) AS NOT MATERIALIZED(SELECT 2) SELECT 1;",
        "WITH c AS(SELECT 1), /* sibling */ c AS(SELECT 2) SELECT 1;",
        "WITH written AS(INSERT INTO duplicate_cte_rows VALUES(1) RETURNING id),"
        "written AS(INSERT INTO duplicate_cte_rows VALUES(2) RETURNING id) SELECT id FROM written;",
        "WITH written AS(INSERT INTO duplicate_cte_rows VALUES(1) RETURNING id),"
        "later AS(WITH c AS(SELECT 1),c AS(SELECT 2) SELECT 1) SELECT 1;",
        "WITH written AS(INSERT INTO duplicate_cte_rows VALUES(1) RETURNING id) "
        "SELECT * FROM (WITH c AS(SELECT 1),c AS(SELECT 2) SELECT 1) d;",
        "INSERT INTO duplicate_cte_rows SELECT * FROM "
        "(WITH c AS(SELECT 1),c AS(SELECT 2) SELECT 1) d;",
    )
    try:
        assert query("CREATE TABLE duplicate_cte_rows(id INT);")[0][1] is None
        for parse_only in (False, True):
            for sql in bad:
                invalid(sql, parse_only, b"I")
                assert query("SELECT id FROM duplicate_cte_rows;")[0][0] == []
            assert query("BEGIN;")[1][-1] == (b"Z", b"T")
            assert query("INSERT INTO duplicate_cte_rows VALUES(7);")[0][1] is None
            invalid(bad[6], parse_only, b"E")
            invalid(bad[6], parse_only, b"E", "25P02")
            assert query("SELECT 1;")[0][1] == "25P02"
            assert query("ROLLBACK;")[1][-1] == (b"Z", b"I")
            assert query("SELECT id FROM duplicate_cte_rows;")[0][0] == []
            assert query("BEGIN;")[1][-1] == (b"Z", b"T")
            assert query("INSERT INTO duplicate_cte_rows VALUES(7);")[0][1] is None
            assert query("SAVEPOINT before_duplicate;")[0][1] is None
            invalid(bad[7], parse_only, b"E")
            assert query("ROLLBACK TO before_duplicate;")[1][-1] == (b"Z", b"T")
            assert query("SELECT id FROM duplicate_cte_rows;")[0][0] == [["7"]]
            assert query("ROLLBACK;")[1][-1] == (b"Z", b"I")
        result, messages = query("INSERT INTO duplicate_cte_rows VALUES(9); " + bad[0])
        assert result[1] == "42712" and messages[-1] == (b"Z", b"I"), result
        assert query("SELECT id FROM duplicate_cte_rows;")[0][0] == []
        for sql, rows, headers, types in (
                ('WITH c AS(SELECT 1 AS id),"C" AS(SELECT 2 AS id) SELECT id FROM c;', [["1"]], ["id"], [23]),
                ('WITH c AS(SELECT 1 AS id),"C" AS(SELECT 2 AS id) SELECT id FROM "C";', [["2"]], ["id"], [23]),
                ("WITH c AS(SELECT 1 AS id),x AS(WITH c AS(SELECT 2 AS id) SELECT id FROM c) SELECT id FROM x;",
                 [["2"]], ["id"], [23]),
                ("SELECT $$WITH c AS(SELECT 1),c AS(SELECT 2)$$ AS value;",
                 [["WITH c AS(SELECT 1),c AS(SELECT 2)"]], ["value"], [25]),
                ("SELECT 'WITH c AS(SELECT 1),c AS(SELECT 2)' AS value;",
                 [["WITH c AS(SELECT 1),c AS(SELECT 2)"]], ["value"], [25])):
            result, _ = query(sql)
            assert (result[0], result[1], result[3], result[5]) == (rows, None, headers, types), (sql, result)
        print("[CTE DUPLICATE NAME PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
