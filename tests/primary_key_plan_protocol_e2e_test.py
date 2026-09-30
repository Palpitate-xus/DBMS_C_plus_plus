#!/usr/bin/env python3
"""Single-column and composite primary keys need different access paths."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "pk_plan_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def expect(sql, rows=None):
        actual, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if rows is not None:
            assert actual == rows, (sql, actual, rows)
        return actual

    try:
        expect("CREATE TABLE single_pk (id INT,value INT,PRIMARY KEY(id));")
        expect("INSERT INTO single_pk VALUES (1,7);")
        expect("CREATE INDEX single_value_idx ON single_pk(value);")
        for where in ("id=1", "id=1 AND value=7"):
            expect("SELECT id,value FROM single_pk WHERE " + where + ";", [["1", "7"]])
        expect("CREATE TABLE empty_pk (id VARCHAR(20) PRIMARY KEY);")
        expect("INSERT INTO empty_pk VALUES ('');")
        expect("SELECT id FROM empty_pk WHERE id='';", [[""]])
        expect("CREATE TABLE pair_pk (a INT,b INT,PRIMARY KEY(a,b));")
        expect("INSERT INTO pair_pk VALUES (1,2),(1,3);")
        query = "SELECT a,b FROM pair_pk WHERE a=1 ORDER BY b;"
        expect(query, [["1", "2"], ["1", "3"]])
        plan = expect("EXPLAIN " + query)
        assert "TableScan" in str(plan) and "IndexScan" not in str(plan), plan
        expect("CREATE INDEX pair_a_idx ON pair_pk(a);")
        expect(query, [["1", "2"], ["1", "3"]])
        # Reuse the exact key cached before DDL; the new standalone secondary
        # index must also invalidate the composite-key table's old plan.
        plan = expect("EXPLAIN " + query)
        assert "IndexScan" in str(plan), plan
        assert "[plan cache hit]" not in str(plan), plan
        expect("SELECT a,b FROM pair_pk WHERE a=1 AND b=2;", [["1", "2"]])
        print("[PRIMARY KEY PLAN PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
