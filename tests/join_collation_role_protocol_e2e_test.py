#!/usr/bin/env python3
"""JOIN serialization keeps COLLATE labels postfix, separate from value bindings."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("join_collation_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def command(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    try:
        command('CREATE TABLE jc_l(id INT,t TEXT,"C" TEXT);')
        command('CREATE TABLE jc_r(id INT,t TEXT,"C" TEXT);')
        command("INSERT INTO jc_l VALUES(1,'a','value-left'),(2,NULL,'null-left'),(3,'','empty-left');")
        command("INSERT INTO jc_r VALUES(10,'a','value-right'),(20,NULL,'null-right'),(30,'','empty-right');")
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON l.t IS NOT DISTINCT FROM r.t ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (l.t COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l LEFT JOIN jc_r r ON (l.t COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") AND r.id=10 ORDER BY l.id;', [["1", "10"], ["2", None], ["3", None]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (CASE WHEN TRUE THEN l.t ELSE NULL END COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (l."C" COLLATE "C") IS DISTINCT FROM (r."C" COLLATE "C") AND l.id=1 ORDER BY r.id;', [["1", "10"], ["1", "20"], ["1", "30"]], [23, 23])
        command("CREATE TABLE jc_empty(id INT,t TEXT);")
        for table in ("jc_l", "jc_empty"):
            sql = f'SELECT l.id FROM {table} l JOIN jc_r r ON (l.t COLLATE "missing_join_collation") IS NOT DISTINCT FROM r.t;'
            result = query(sql)
            assert result[1] == "42704" and result[0] == [], (sql, result)
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (l.t COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        print("[JOIN COLLATION ROLE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
