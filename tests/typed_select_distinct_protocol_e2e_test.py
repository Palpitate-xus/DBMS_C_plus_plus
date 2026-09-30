#!/usr/bin/env python3
"""DISTINCT and DISTINCT ON use typed keys without rewriting display values."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "typed_distinct_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE typed_distinct (id INT,v NUMERIC,t TEXT);")
        query("INSERT INTO typed_distinct VALUES (1,0.50,'same'),(2,0.500,'same'),"
              "(3,2.00,''),(4,NULL,'NULL'),(5,NULL,NULL);")
        assert query("SELECT DISTINCT v FROM typed_distinct ORDER BY v;") == [["0.50"], ["2.00"], [None]]
        assert query("SELECT DISTINCT v AS amount FROM typed_distinct ORDER BY amount;") == [["0.50"], ["2.00"], [None]]
        assert query("SELECT DISTINCT t FROM typed_distinct ORDER BY t;") == [[""], ["NULL"], ["same"], [None]]
        assert query("SELECT DISTINCT v,t FROM typed_distinct ORDER BY v,t;") == [["0.50", "same"], ["2.00", ""], [None, "NULL"], [None, None]]
        assert query("SELECT DISTINCT t,v AS amount FROM typed_distinct ORDER BY t,amount;") == [["", "2.00"], ["NULL", None], ["same", "0.50"], [None, None]]
        assert query("SELECT DISTINCT ON (v) v,id FROM typed_distinct ORDER BY v,id;") == [["0.50", "1"], ["2.00", "3"], [None, "4"]]
        assert query("SELECT DISTINCT v FROM typed_distinct ORDER BY v LIMIT 1 OFFSET 1;") == [["2.00"]]
        query("CREATE TABLE typed_distinct_exact (v NUMERIC);")
        query("INSERT INTO typed_distinct_exact VALUES (123456789012345678901234567890.1),"
              "(123456789012345678901234567890.10),(123456789012345678901234567890.2);")
        assert query("SELECT DISTINCT v FROM typed_distinct_exact ORDER BY v;") == [["123456789012345678901234567890.1"], ["123456789012345678901234567890.2"]]
        assert query("SELECT 42;") == [["42"]]
        print("[TYPED SELECT DISTINCT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
