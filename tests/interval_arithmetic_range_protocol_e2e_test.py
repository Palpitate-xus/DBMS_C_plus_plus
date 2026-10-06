#!/usr/bin/env python3
"""Interval operations must not silently return NULL or invalid-width values."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("interval_arithmetic_runner", root / "tests/compat/pg_diff_runner.py")
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
        server = {"sock": sock}
        print("PG17.2 diagnostic reference", version[0], flush=True)
    else:
        server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in ("CREATE TEMP TABLE interval_arithmetic_rows(id INT,d DATE);",
                    "INSERT INTO interval_arithmetic_rows VALUES(1,'2024-02-29'),(2,NULL);"):
            assert query(sql)[1] is None, sql
        for expression in (
            "INTERVAL '9223372036854775807 microseconds'+INTERVAL '1 microsecond'",
            "INTERVAL '-9223372036854775808 microseconds'-INTERVAL '1 microsecond'",
            "INTERVAL '2147483647 months'+INTERVAL '1 month'",
            "INTERVAL '-2147483648 months'-INTERVAL '1 month'",
            "INTERVAL '2147483647 days'+INTERVAL '1 day'",
            "INTERVAL '-2147483648 days'-INTERVAL '1 day'",
            "INTERVAL '2147483647 months'*2",
            "INTERVAL '2147483647 days'*2",
            "INTERVAL '3000000000 microseconds'*4000000000",
            "INTERVAL '-2147483648 months' / -1",
            "INTERVAL '-2147483648 days' / -1",
            "INTERVAL '-9223372036854775808 microseconds' * -1",
        ):
            result = query("SELECT " + expression + ";")
            assert result[1] == "22008" and result[0] == [], (expression, result)
            assert query("SELECT 7;")[0] == [["7"]]
        for expression, value in (
            ("INTERVAL '-9223372036854775807 microseconds'-INTERVAL '1 microsecond'", "-2562047788:00:54.775808"),
            ("INTERVAL '9223372036854775806 microseconds'+INTERVAL '1 microsecond'", "2562047788:00:54.775807"),
            ("INTERVAL '-9223372036854775808 microseconds'+INTERVAL '0 microseconds'", "-2562047788:00:54.775808"),
            ("INTERVAL '-2147483648 months'+INTERVAL '0 months'", "-178956970 years -8 mons"),
            ("INTERVAL '-2147483648 days'+INTERVAL '0 days'", "-2147483648 days"),
            ("INTERVAL '2 days'*2", "4 days"),
            ("INTERVAL '4 hours'/2", "02:00:00"),
            ("CAST(NULL AS INTERVAL)+INTERVAL '1 day'", None),
        ):
            result = query("SELECT " + expression + ";")
            assert result[1] is None and result[0] == [[value]] and result[5] == [1186], (expression, result)
        for sql, rows in (
            ("SELECT d+(INTERVAL '2 days'+INTERVAL '1 day') FROM interval_arithmetic_rows WHERE id=1;", [["2024-03-03 00:00:00"]]),
            ("SELECT d+(INTERVAL '2 days'+INTERVAL '1 day') FROM interval_arithmetic_rows WHERE id=2;", [[None]]),
        ):
            result = query(sql)
            assert result[1] is None and result[0] == rows, (sql, result)
        for row in (1,2):
            sql = ("SELECT d+(INTERVAL '9223372036854775807 microseconds'+INTERVAL '1 microsecond') "
                   f"FROM interval_arithmetic_rows WHERE id={row};")
            result = query(sql)
            assert result[1] == "22008" and result[0] == [], (sql, result)
        assert query("BEGIN;")[1] is None
        result = query("SELECT INTERVAL '2147483647 months'+INTERVAL '1 month';")
        assert result[1] == "22008", result
        result = query("SELECT 7;")
        assert result[1] == "25P02", result
        assert query("ROLLBACK;")[1] is None
        assert query("SELECT 7;")[0] == [["7"]]
        print("[INTERVAL ARITHMETIC RANGE " + ("PG17.2 DIAGNOSTIC" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__ == "__main__":
    main()
