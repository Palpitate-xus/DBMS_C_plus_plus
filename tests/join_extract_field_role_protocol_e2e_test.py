#!/usr/bin/env python3
"""EXTRACT fields in JOIN operands are syntax labels, not range-column references."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("join_extract_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    try:
        for sql in ("CREATE TABLE je_l(id INT,day DATE,year INT);", "CREATE TABLE je_r(id INT,day DATE,year INT);", "CREATE TABLE je_empty(id INT,day DATE);", "INSERT INTO je_l VALUES(1,'2026-10-06',9999),(2,NULL,9998);", "INSERT INTO je_r VALUES(10,'2026-01-01',8888),(20,NULL,8887);"):
            result = query(sql)
            assert result[1] is None, (sql, result)
        expect("SELECT l.id,r.id FROM je_l l JOIN je_r r ON l.year IS DISTINCT FROM r.year ORDER BY l.id,r.id;", [["1", "10"], ["1", "20"], ["2", "10"], ["2", "20"]], [23, 23])
        expect("SELECT l.id,r.id FROM je_l l JOIN je_r r ON EXTRACT(year FROM l.day) IS NOT DISTINCT FROM EXTRACT(year FROM r.day) ORDER BY l.id;", [["1", "10"], ["2", "20"]], [23, 23])
        expect('SELECT l.id,r.id FROM je_l l JOIN je_r r ON EXTRACT("YEAR" FROM l.day) IS NOT DISTINCT FROM EXTRACT(year FROM r.day) ORDER BY l.id;', [["1", "10"], ["2", "20"]], [23, 23])
        expect("SELECT l.id,r.id FROM je_l l JOIN je_r r ON CAST(EXTRACT(year FROM l.day) AS INT) IS NOT DISTINCT FROM CAST(EXTRACT(year FROM r.day) AS INT) ORDER BY l.id;", [["1", "10"], ["2", "20"]], [23, 23])
        expect("SELECT l.id,r.id FROM je_l l LEFT JOIN je_r r ON EXTRACT(month FROM l.day) IS NOT DISTINCT FROM EXTRACT(month FROM r.day) ORDER BY l.id;", [["1", None], ["2", "20"]], [23, 23])
        expect("SELECT l.id,r.id FROM je_empty l JOIN je_r r ON EXTRACT(year FROM l.day) IS NOT DISTINCT FROM EXTRACT(year FROM r.day);", [], [23, 23])
        result = query("SELECT l.id FROM je_l l JOIN je_r r ON EXTRACT(year FROM z.day) IS NOT DISTINCT FROM EXTRACT(year FROM r.day);")
        assert result[1] == "42P01", result
        expect("SELECT l.id,r.id FROM je_l l JOIN je_r r ON date_part('year',l.day) IS NOT DISTINCT FROM date_part('year',r.day) ORDER BY l.id;", [["1", "10"], ["2", "20"]], [23, 23])
        for sql in ("CREATE TABLE je_dynamic(id INT,field TEXT,day DATE);", "INSERT INTO je_dynamic VALUES(1,'year','2026-10-06'),(2,NULL,NULL);"):
            assert query(sql)[1] is None
        for function in ("date_part", '"extract"'):
            expect(f"SELECT l.id,r.id FROM je_dynamic l JOIN je_r r ON {function}(l.field,l.day) IS NOT DISTINCT FROM EXTRACT(year FROM r.day) ORDER BY l.id;", [["1", "10"], ["2", "20"]], [23, 23])
        print("[JOIN EXTRACT FIELD ROLE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
