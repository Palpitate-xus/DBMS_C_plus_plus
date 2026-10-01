#!/usr/bin/env python3
"""FOR UPDATE locks selected rows, not the gaps occupied by other new keys."""

import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "row_gap_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        first = socket.create_connection((host, port), timeout=120)
        client.startup_reference(first, user, database, password)
        runner.verify_reference_version(client, first)
        server = {"sock": first, "port": port}
    else:
        server = runner.start_ours(client)
    second = None
    table = "row_gap_" + uuid.uuid4().hex
    created = False

    def query(sock, sql, rows=None, state=None, ready=b"I"):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    try:
        first = server["sock"]
        second = socket.create_connection((host if reference else "127.0.0.1", server["port"]), timeout=120)
        if reference:
            client.startup_reference(second, user, database, password)
        else:
            client.startup(second, "alice", "info")
        query(first, f"CREATE TABLE {table}(id INT PRIMARY KEY,v INT);")
        created = True
        query(second, "SET lock_timeout=50;")
        for isolation in ("READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"):
            query(second, f"TRUNCATE {table};")
            query(second, f"INSERT INTO {table} VALUES(1,1),(10,10);")
            query(first, f"BEGIN ISOLATION LEVEL {isolation};", ready=b"T")
            query(first, f"SELECT id FROM {table} WHERE id=1 FOR UPDATE;", [["1"]], ready=b"T")
            # Keys on both sides of the selected row must remain insertable.
            for key in (0, 2, 11):
                query(second, f"INSERT INTO {table} VALUES({key},{key}) RETURNING id;", [[str(key)]])
            # Keep the actual row exclusion: only a different row is writable.
            query(second, f"UPDATE {table} SET v=999 WHERE id=1;", [], "55P03")
            query(second, f"DELETE FROM {table} WHERE id=1;", [], "55P03")
            query(second, f"UPDATE {table} SET v=3 WHERE id=2 RETURNING id,v;", [["2", "3"]])
            query(first, "ROLLBACK;")
            query(second, f"SELECT id,v FROM {table} ORDER BY id;",
                  [["0", "0"], ["1", "1"], ["2", "3"], ["10", "10"], ["11", "11"]])
        print("[FOR UPDATE INSERT GAP " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if second is not None:
            try:
                client.simple_query(second, "ROLLBACK;")
            except Exception:
                pass
            second.close()
        if reference:
            try:
                client.simple_query(server["sock"], "ROLLBACK;")
                if created:
                    client.simple_query(server["sock"], f"DROP TABLE {table};")
            finally:
                server["sock"].close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
