#!/usr/bin/env python3
"""A DML lock error must preserve earlier work and user SAVEPOINT recovery."""

import importlib.util
from pathlib import Path
import socket
import sys
import threading
import time
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "dml_savepoint_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    prefix = "dml_sp_" + uuid.uuid4().hex
    table_names = {"lock_sp_rows": prefix + "_rows",
                   "lock_sp_prior": prefix + "_prior"}
    created = []

    def wire_sql(sql):
        for placeholder, table in table_names.items():
            sql = sql.replace(placeholder, table)
        return sql

    def query(sock, sql, expected=None, ready=b"I"):
        messages = client.simple_query(sock, wire_sql(sql))
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    def error(sock, sql, expected):
        messages = client.simple_query(sock, wire_sql(sql))
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == [] and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", b"E"), (sql, messages[-1])

    try:
        second = socket.create_connection((host if reference else "127.0.0.1", server["port"]), timeout=120)
        if reference:
            client.startup_reference(second, user, database, password)
        else:
            client.startup(second, "alice", "info")
        first = server["sock"]
        query(first, "CREATE TABLE lock_sp_rows(id INT PRIMARY KEY,v INT);")
        created.append(table_names["lock_sp_rows"])
        query(first, "CREATE TABLE lock_sp_prior(id INT PRIMARY KEY,v INT);")
        created.append(table_names["lock_sp_prior"])
        query(first, "INSERT INTO lock_sp_rows VALUES(1,1);")
        cases = ((2, "UPDATE lock_sp_rows SET v=999 WHERE id=1;", "user_lock_sp"),
                 (3, "DELETE FROM lock_sp_rows WHERE id=1;", "user_lock_sp"),
                 (4, "UPDATE lock_sp_rows SET v=999 WHERE id=1;", "__dbms_dml_statement_0"),
                 (5, "DELETE FROM lock_sp_rows WHERE id=1;", "__dbms_delete_statement_0"))
        for row_id, action, savepoint in cases:
            query(first, "BEGIN;", ready=b"T")
            query(first, "SELECT id FROM lock_sp_rows WHERE id=1 FOR UPDATE;", [["1"]], b"T")
            query(second, "BEGIN;", ready=b"T")
            query(second, "SET lock_timeout=50;", ready=b"T")
            query(second, f"INSERT INTO lock_sp_prior VALUES({row_id},{row_id});", ready=b"T")
            query(second, f"SAVEPOINT {savepoint};", ready=b"T")
            query(second, f"INSERT INTO lock_sp_prior VALUES({row_id + 100},0);", ready=b"T")
            error(second, action, "55P03")
            error(second, "SELECT id FROM lock_sp_rows;", "25P02")
            query(second, f"ROLLBACK TO SAVEPOINT {savepoint};", ready=b"T")
            query(second, f"SELECT id,v FROM lock_sp_prior WHERE id={row_id};", [[str(row_id), str(row_id)]], b"T")
            query(second, f"SELECT id FROM lock_sp_prior WHERE id={row_id + 100};", [], b"T")
            query(second, "COMMIT;")
            query(first, "ROLLBACK;")
            query(second, "SELECT v FROM lock_sp_rows WHERE id=1;", [["1"]])
        query(first, "INSERT INTO lock_sp_rows VALUES(2,2);")
        for row_id, action in ((6, "UPDATE lock_sp_rows SET v=999 WHERE id=1;"),
                               (7, "DELETE FROM lock_sp_rows WHERE id=1;")):
            query(first, "BEGIN;", ready=b"T")
            query(first, "SET lock_timeout=5000;", ready=b"T")
            query(first, "SET deadlock_timeout=2000;", ready=b"T")
            query(first, "SELECT id FROM lock_sp_rows WHERE id=1 FOR UPDATE;", [["1"]], b"T")
            query(second, "BEGIN;", ready=b"T")
            query(second, "SET lock_timeout=5000;", ready=b"T")
            query(second, "SET deadlock_timeout=20;", ready=b"T")
            query(second, f"INSERT INTO lock_sp_prior VALUES({row_id},{row_id});", ready=b"T")
            query(second, "SAVEPOINT deadlock_recovery;", ready=b"T")
            query(second, "SELECT id FROM lock_sp_rows WHERE id=2 FOR UPDATE;", [["2"]], b"T")
            waiter_errors = []

            def wait_for_row():
                try:
                    query(first, "SELECT id FROM lock_sp_rows WHERE id=2 FOR UPDATE;", [["2"]], b"T")
                except Exception as exc:
                    waiter_errors.append(exc)

            waiter = threading.Thread(target=wait_for_row, daemon=True)
            waiter.start()
            time.sleep(0.1)
            error(second, action, "40P01")
            error(second, "SELECT 1;", "25P02")
            query(second, "ROLLBACK TO SAVEPOINT deadlock_recovery;", ready=b"T")
            waiter.join(timeout=5)
            assert not waiter.is_alive() and not waiter_errors, waiter_errors
            query(second, f"SELECT id,v FROM lock_sp_prior WHERE id={row_id};",
                  [[str(row_id), str(row_id)]], b"T")
            query(second, "COMMIT;")
            query(first, "ROLLBACK;")
            query(second, "SELECT id,v FROM lock_sp_rows ORDER BY id;", [["1", "1"], ["2", "2"]])
        query(second, "SELECT id,v FROM lock_sp_prior ORDER BY id;",
              [[str(i), str(i)] for i in range(2, 8)])
        print("[DML LOCK ERROR SAVEPOINT " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
                for table in reversed(created):
                    client.simple_query(server["sock"], f"DROP TABLE {table};")
            finally:
                server["sock"].close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
