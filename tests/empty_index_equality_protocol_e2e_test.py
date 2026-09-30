#!/usr/bin/env python3
"""Incomplete legacy indexes must not hide non-NULL empty text values."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "empty_index_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    try:
        query("CREATE TABLE empty_index_values(id INT PRIMARY KEY,k TEXT);")
        query("INSERT INTO empty_index_values VALUES (1,''),(2,NULL),(3,'x');")
        query("CREATE INDEX empty_index_k ON empty_index_values(k);")
        query("INSERT INTO empty_index_values VALUES (4,'');")
        assert query("SELECT id,k FROM empty_index_values WHERE k='' AND length(k)=0 ORDER BY id;") == [["1", ""], ["4", ""]]
        rows = query("EXPLAIN (FORMAT JSON,ANALYZE TRUE) SELECT * FROM empty_index_values WHERE k='';")
        document = json.loads("\n".join(row[0] for row in rows))
        assert document["actualRows"] == 2, document
        assert query("SELECT id,k FROM empty_index_values WHERE k='' AND id=1 AND length(k)=0;") == [["1", ""]]
        assert query("SELECT id,k FROM empty_index_values WHERE (k='' OR id=3) AND length(k)>=0 ORDER BY id;") == [["1", ""], ["3", "x"], ["4", ""]]
        assert query("SELECT 42;") == [["42"]]
        print("[EMPTY INDEX EQUALITY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
