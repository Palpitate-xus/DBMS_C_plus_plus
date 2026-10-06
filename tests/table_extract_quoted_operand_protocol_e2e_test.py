#!/usr/bin/env python3
"""Direct table EXTRACT dispatch must bind delimited source columns."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("table_extract_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in ('CREATE TABLE table_extract_rows(id INT,"D" DATE,day DATE,year INT);',
                    "INSERT INTO table_extract_rows VALUES(1,'2026-10-06','2026-10-06',9999),(2,'2026-10-06','2026-10-06',NULL),(3,NULL,NULL,8888);"):
            assert query(sql)[1] is None, sql
        for sql, rows in (
            ('SELECT EXTRACT(year FROM "D") FROM table_extract_rows WHERE id=1;', [["2026"]]),
            ('SELECT EXTRACT(year FROM day) FROM table_extract_rows WHERE id=1;', [["2026"]]),
            ('SELECT EXTRACT(year FROM "D") FROM table_extract_rows WHERE id=2;', [["2026"]]),
            ('SELECT EXTRACT(year FROM "D") FROM table_extract_rows WHERE id=3;', [[None]]),
            ('SELECT EXTRACT(year FROM r."D") FROM table_extract_rows r WHERE id=1;', [["2026"]]),
            ('SELECT EXTRACT("YEAR" FROM "D") FROM table_extract_rows WHERE id=1;', [["2026"]]),
            ("SELECT EXTRACT('year' FROM \"D\") FROM table_extract_rows WHERE id=1;", [["2026"]]),
            ('SELECT EXTRACT(year FrOm "D") FROM table_extract_rows WHERE id=1;', [["2026"]]),
            ('SELECT EXTRACT (year\tFROM\t"D") FROM table_extract_rows WHERE id=1;', [["2026"]]),
            ('SELECT EXTRACT(year FROM CAST("D" AS TIMESTAMP)) FROM table_extract_rows WHERE id=1;', [["2026"]]),
            ('SELECT EXTRACT(year FROM CASE WHEN id=1 THEN "D" ELSE day END) FROM table_extract_rows WHERE id=1;', [["2026"]]),
        ):
            result = query(sql)
            assert result[1] is None and result[0] == rows and result[5] == [1700], (sql, result)
        for sql, state in (
            ('SELECT EXTRACT(NoSuchUnit FROM "D") FROM table_extract_rows WHERE id=1;', '22023'),
            ('SELECT EXTRACT(hour FROM "D") FROM table_extract_rows WHERE id=1;', '0A000'),
            ('SELECT EXTRACT(year FROM missing) FROM table_extract_rows WHERE id=1;', '42703'),
        ):
            result = query(sql)
            assert result[1] == state and result[0] == [], (sql, result)
        for sql in ('CREATE TABLE table_extract_effect(id INT);',
                    'CREATE FUNCTION table_extract_source(d DATE,i INT) RETURNS DATE LANGUAGE plpgsql AS $$BEGIN INSERT INTO table_extract_effect VALUES(i); RETURN d; END;$$;'):
            assert query(sql)[1] is None, sql
        result = query('SELECT EXTRACT(year FROM table_extract_source("D",id)) FROM table_extract_rows WHERE id=1;')
        assert result[1] is None and result[0] == [["2026"]] and result[5] == [1700], result
        result = query('SELECT id FROM table_extract_effect;')
        assert result[1] is None and result[0] == [["1"]], result
        print("[TABLE EXTRACT QUOTED OPERAND PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
