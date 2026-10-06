#!/usr/bin/env python3
"""Typed table expressions keep delimited column identities separate."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("quoted_row_runner", root / "tests/compat/pg_diff_runner.py")
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
        for sql in ('CREATE TABLE qr(id INT,"F" BIGINT,f INT,"T" TEXT,t TEXT,"D" DATE,year INT);',
                    "INSERT INTO qr VALUES(1,2,1,'00123','x','2026-10-06',1),(2,NULL,2,'','y',NULL,1);"):
            assert query(sql)[1] is None, sql
        # Cast forces the already-supported typed scalar path: this fixture
        # does not rely on the separately fixed ordinary arithmetic bridge.
        expect('SELECT CAST("F" AS BIGINT)+f FROM qr WHERE id=1;', [["3"]], [20])
        expect('SELECT CAST("F" AS BIGINT)+F FROM qr WHERE id=1;', [["3"]], [20])
        expect('SELECT CAST("F" AS BIGINT)+f FROM qr WHERE id=2;', [[None]], [20])
        expect('SELECT CAST("T" AS TEXT)||\':\'||CAST(f AS TEXT) FROM qr WHERE id=1;', [["00123:1"]], [25])
        expect('SELECT CAST("T" AS TEXT)||t FROM qr WHERE id=2;', [["y"]], [25])
        expect('SELECT CAST(r."F" AS BIGINT)+r.f FROM qr r WHERE id=1;', [["3"]], [20])
        expect('SELECT CASE WHEN "F"=2 THEN f+10 ELSE 99 END FROM qr WHERE id=1;', [["11"]], [23])
        expect('SELECT CAST(EXTRACT(year FROM "D") AS NUMERIC) FROM qr WHERE id=1;', [["2026"]], [1700])
        print("[EXPRESSION QUOTED ROW BINDING PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
