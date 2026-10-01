#!/usr/bin/env python3
"""BIGINT and NUMERIC sums retain every digit outside the int64 range."""

import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "exact_sum_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    prefix = "exact_sum_" + uuid.uuid4().hex
    tables = []
    minimum, maximum = "-9223372036854775808", "9223372036854775807"

    def query(sql, rows=None):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] is None, (sql, result)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    try:
        table = prefix + "_bigint"
        query(f"CREATE TABLE {table}(grp INT,v BIGINT);")
        tables.append(table)
        query(f"INSERT INTO {table} VALUES(1,{maximum}),(1,{maximum}),(1,NULL),"
              f"(2,{minimum}),(2,{minimum}),(3,{maximum}),(3,{minimum}),(4,NULL);")
        query(f"SELECT SUM(v),AVG(v) FROM {table} WHERE grp=1;",
              [["18446744073709551614", maximum]])
        query(f"SELECT SUM(v),AVG(v) FROM {table} WHERE grp=2;",
              [["-18446744073709551616", minimum]])
        query(f"SELECT SUM(v) FROM {table};", [["-3"]])
        query(f"SELECT grp,SUM(v),COUNT(v) FROM {table} GROUP BY grp ORDER BY grp;",
              [["1", "18446744073709551614", "2"],
               ["2", "-18446744073709551616", "2"], ["3", "-1", "2"], ["4", None, "0"]])
        query(f"SELECT SUM(v) FROM {table} WHERE grp=99;", [[None]])
        numeric = prefix + "_numeric"
        query(f"CREATE TABLE {numeric}(v NUMERIC);")
        tables.append(numeric)
        value = "12345678901234567890.12345678901234567890"
        query(f"INSERT INTO {numeric} VALUES({value}),({value}),(NULL);")
        query(f"SELECT SUM(v) FROM {numeric};", [["24691357802469135780.24691357802469135780"]])
        floating = prefix + "_double"
        query(f"CREATE TABLE {floating}(v DOUBLE PRECISION);")
        tables.append(floating)
        query(f"INSERT INTO {floating} VALUES(1.5),(2.5),(NULL);")
        query(f"SELECT SUM(v) FROM {floating};", [["4"]])
        for name in reversed(tables):
            query(f"DROP TABLE {name};")
        tables.clear()
        print("[EXACT SUM " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK;")
                for name in reversed(tables):
                    client.simple_query(sock, f"DROP TABLE {name};")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
