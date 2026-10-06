#!/usr/bin/env python3
"""Typed INTERVAL inputs preserve bounded fields and SQLSTATE, never NULL on overflow."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("typed_interval_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        version = runner.decode_wire_result(client.simple_query(sock, "SHOW server_version_num;"))
        assert version[1] is None and version[0] == [["170002"]], version
        print("PG17.2 diagnostic reference", version[0], flush=True)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in ("CREATE TEMP TABLE interval_range_rows(id INT,d DATE);",
                    "INSERT INTO interval_range_rows VALUES(1,'2024-02-29'),(2,NULL);"):
            assert query(sql)[1] is None, sql
        for text in ("9223372036854775807 months", "9223372036854775807 years",
                     "9223372036854775807 weeks", "2147483648 months", "-2147483649 days",
                     "9223372036854775808 microseconds", "P9223372036854775807Y",
                     "@ 9223372036854775807 months"):
            for sql in (f"SELECT INTERVAL '{text}';",
                        f"SELECT CAST('{text}' AS INTERVAL);",
                        f"SELECT DATE '2024-02-29'+INTERVAL '{text}';",
                        f"SELECT d+INTERVAL '{text}' FROM interval_range_rows WHERE id=1;",
                        f"SELECT d+INTERVAL '{text}' FROM interval_range_rows WHERE id=2;"):
                result = query(sql)
                assert result[1] == "22015" and result[0] == [], (sql, result)
                recovery = query("SELECT 7;")
                assert recovery[1] is None and recovery[0] == [["7"]], recovery
        for sql, rows in (
            ("SELECT DATE '2024-02-29'+INTERVAL '1 month';", [["2024-03-29 00:00:00"]]),
            ("SELECT DATE '2024-02-29'+INTERVAL 'P1D';", [["2024-03-01 00:00:00"]]),
            ("SELECT d+INTERVAL '1 week' FROM interval_range_rows WHERE id=1;", [["2024-03-07 00:00:00"]]),
            ("SELECT d+INTERVAL '1 week' FROM interval_range_rows WHERE id=2;", [[None]]),
            ("SELECT CAST(NULL AS INTERVAL);", [[None]]),
            ("SELECT date_trunc('microseconds',INTERVAL '-9223372036854775808 microseconds');",
             [["-2562047788:00:54.775808"]]),
        ):
            result = query(sql)
            assert result[1] is None and result[0] == rows, (sql, result)
        for text in ("2147483647 months", "-2147483648 months", "2147483647 days",
                     "-2147483648 days", "9223372036854775807 microseconds",
                     "-9223372036854775808 microseconds"):
            result = query(f"SELECT INTERVAL '{text}';")
            assert result[1] is None and len(result[0]) == 1 and result[0][0][0] is not None, (text, result)
        for sql in ("SELECT INTERVAL '178956971 years';", "SELECT CAST('178956971 years' AS INTERVAL);"):
            result = query(sql)
            assert result[1] == "22008" and result[0] == [], (sql, result)
        for sql in ("SELECT INTERVAL '" + "9" * 400 + " months';",
                    "SELECT EXTRACT(epoch FROM INTERVAL '" + "9" * 400 + " months');"):
            result = query(sql)
            assert result[1] == "22007" and result[0] == [], (sql, result)
        print("[TYPED INTERVAL RANGE " + ("PG17.2 DIAGNOSTIC" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
