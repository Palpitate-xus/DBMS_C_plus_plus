#!/usr/bin/env python3
"""GROUP BY type equality keeps representative values and physical NULLs."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "typed_group_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE typed_group (v NUMERIC,t TEXT);")
        query("INSERT INTO typed_group VALUES (0.50,'0.50'),(0.500,'0.500'),"
              "(2.00,''),(NULL,'NULL'),(NULL,NULL);")
        assert query("SELECT v,count(*) FROM typed_group GROUP BY v ORDER BY v;") == [["0.50", "2"], ["2.00", "1"], [None, "2"]]
        assert query("SELECT t,count(*) FROM typed_group GROUP BY t ORDER BY t;") == [["", "1"], ["0.50", "1"], ["0.500", "1"], ["NULL", "1"], [None, "1"]]
        assert query("SELECT v,count(*) FROM typed_group GROUP BY v HAVING count(*)=2 ORDER BY v;") == [["0.50", "2"], [None, "2"]]
        assert query("SELECT v,count(*) FROM typed_group GROUP BY GROUPING SETS ((v),()) ORDER BY count(*),v;") == [["2.00", "1"], ["0.50", "2"], [None, "2"], [None, "5"]]
        assert query("SELECT 42;") == [["42"]]
        print("[TYPED GROUP KEY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
