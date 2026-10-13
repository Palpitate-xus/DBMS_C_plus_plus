#!/usr/bin/env python3
"""Exact window SUM/AVG for integer limits, fractions and NULL/empty frames."""
import importlib.util
import socket
import sys
from decimal import Decimal
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "window_exact_runner", root / "tests/compat/pg_diff_runner.py")
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server["sock"]

    def query(sql):
        result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
        assert result[1] is None, (sql, result)
        return result

    def numeric(rows):
        return [[None if value is None else Decimal(value) for value in row] for row in rows]

    controls = 0
    try:
        for sql in [
            "CREATE TEMP TABLE exact_ints(id INT,p TEXT,v BIGINT)",
            "CREATE TEMP TABLE exact_fractions(id INT,p TEXT,v NUMERIC)",
            "CREATE TEMP TABLE exact_empty(id INT,p TEXT,v BIGINT)",
            "INSERT INTO exact_ints VALUES"
            "(1,'maximum',9223372036854775807),(2,'maximum',9223372036854775807),"
            "(3,'minimum',-9223372036854775808),(4,'minimum',-9223372036854775808),"
            "(5,'cancel',9223372036854775807),(6,'cancel',-9223372036854775808),"
            "(7,'null',NULL),(8,'null',NULL)",
            "INSERT INTO exact_fractions VALUES"
            "(1,'fraction',1.25),(2,'fraction',2.75),(3,'mixed',1.25),(4,'mixed',NULL)",
        ]:
            query(sql)
        fixtures = [
            ("exact_ints", [
                ["1", "18446744073709551614", "9223372036854775807"],
                ["2", "18446744073709551614", "9223372036854775807"],
                ["3", "-18446744073709551616", "-9223372036854775808"],
                ["4", "-18446744073709551616", "-9223372036854775808"],
                ["5", "-1", "-0.5"], ["6", "-1", "-0.5"],
                ["7", None, None], ["8", None, None],
            ], ["9223372036854775807", "-9223372036854775808", "9223372036854775807", None]),
            ("exact_fractions", [
                ["1", "4", "2"], ["2", "4", "2"],
                ["3", "1.25", "1.25"], ["4", "1.25", "1.25"],
            ], ["1.25", "1.25"]),
            ("exact_empty", [], []),
        ]
        for table, whole, first_values in fixtures:
            for legacy, cumulative in [(False, False), (False, True),
                                       (True, False), (True, True)]:
                end = "CURRENT ROW" if cumulative else "UNBOUNDED FOLLOWING"
                window = "PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND " + end
                # nth_value is not in convertToVolcanoWindowSpec's supported
                # set, so including it exercises the Main legacy window path.
                extra = (f",nth_value(v,1) OVER ({window}) AS first" if legacy else "")
                final_order = "p,id" if legacy else "id"
                sql = (f"SELECT id,sum(v) OVER ({window}) AS total,"
                       f"avg(v) OVER ({window}) AS average{extra} FROM {table} ORDER BY {final_order}")
                expected = [row[:] for row in whole]
                if cumulative:
                    for index, value in enumerate(first_values):
                        expected[index * 2][1:] = [value, value]
                if legacy:
                    for index, row in enumerate(expected):
                        row.append(first_values[index // 2])
                if legacy and table == "exact_ints":
                    expected = expected[4:6] + expected[:4] + expected[6:]
                result = query(sql)
                valid = (numeric(result[0]) == numeric(expected)
                         and result[3] == ["id", "total", "average"] + (["first"] if legacy else [])
                         and result[4] == f"SELECT {len(expected)}"
                         and result[5] == [23, 1700, 1700] + (
                             [1700 if table == "exact_fractions" else 20] if legacy else []))
                print("WINDOW_EXACT", sql, result, "match", valid, flush=True)
                assert valid, (sql, result, expected)
                controls += 1
        assert controls == 12
        print("[WINDOW EXACT ACCUMULATOR PROTOCOL] 12 controls / 48 rows passed")
    finally:
        if server:
            r.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
