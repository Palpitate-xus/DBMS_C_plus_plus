#!/usr/bin/env python3
"""Equivalent integer spellings cannot create distinct primary keys."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "integer_write_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected_state=None):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected_state, (sql, state, message, rows)
        return rows

    def analyzed(select):
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + select)))
        assert document["actualRows"] == 1, (select, document)

    try:
        query("CREATE TABLE integer_write (id BIGINT PRIMARY KEY,value BIGINT);")
        query("CREATE INDEX integer_write_value ON integer_write(value);")
        query("INSERT INTO integer_write VALUES ('0001','+0007');")
        analyzed("SELECT id FROM integer_write WHERE id=1;")
        analyzed("SELECT id FROM integer_write WHERE value=7;")
        query("INSERT INTO integer_write VALUES (1,7);", "23505")
        query("INSERT INTO integer_write VALUES ('-0','0000');")
        query("INSERT INTO integer_write VALUES (0,0);", "23505")
        assert sorted(query("SELECT id FROM integer_write;")) == [["0"], ["1"]]
        query("BEGIN;")
        query("UPDATE integer_write SET value='+0008' WHERE id=1;")
        analyzed("SELECT id FROM integer_write WHERE value=8;")
        query("ROLLBACK;")
        analyzed("SELECT id FROM integer_write WHERE id=1 AND value=7;")
        query("REINDEX TABLE integer_write;")
        analyzed("SELECT id FROM integer_write WHERE id=1;")
        query("CREATE TABLE integer_write_pair (a BIGINT,b BIGINT,PRIMARY KEY(a,b));")
        query("INSERT INTO integer_write_pair VALUES ('+0001','0002');")
        query("INSERT INTO integer_write_pair VALUES (1,2);", "23505")
        assert query("SELECT a,b FROM integer_write_pair;") == [["1", "2"]]
        assert query("SELECT 42;") == [["42"]]
        print("[INTEGER INDEX WRITE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
