#!/usr/bin/env python3
"""Row write conflicts preserve SAVEPOINT recovery and check the current version."""

import importlib.util
from pathlib import Path
import select
import signal
import socket
import subprocess
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
                   "lock_sp_prior": prefix + "_prior",
                   "lock_sp_versions": prefix + "_versions"}
    created = []

    def wire_sql(sql):
        for placeholder, table in table_names.items():
            sql = sql.replace(placeholder, table)
        return sql

    def query(sock, sql, expected=None, ready=b"I", *, tag=None):
        messages = client.simple_query(sock, wire_sql(sql))
        rows, state, message, _, actual_tag = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        if tag is not None:
            assert actual_tag == tag, (sql, actual_tag, tag)
        return rows

    def error(sock, sql, expected):
        messages = client.simple_query(sock, wire_sql(sql))
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == [] and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", b"E"), (sql, messages[-1])

    def overlapping_writes(holder, waiter, waiter_pid):
        table = "lock_sp_versions"
        query(holder, f"CREATE TABLE {table}(id INT PRIMARY KEY,v INT,cohort INT)", tag="CREATE TABLE")
        created.append(table_names[table])
        seed = ",".join(f"({i},10,{i})" for i in range(1, 12))
        query(holder, f"INSERT INTO {table} VALUES {seed},(999,999,999)", tag="INSERT 0 12")
        query(waiter, "SET lock_timeout=8000", tag="SET")
        survivors = {999: 999}

        def rows(numbers):
            return [[str(key), str(value)] for key, value in numbers]

        def wait_for_holder():
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                assert not select.select([waiter], [], [], 0)[0], "waiter responded before holder released"
                if reference:
                    blockers = query(holder, f"SELECT pg_backend_pid() = ANY(pg_blocking_pids({waiter_pid}))",
                                     ready=b"T")
                    if blockers == [["t"]]:
                        return
                else:
                    locks = query(holder, "SELECT * FROM pg_locks", ready=b"T")
                    if any(row[3:] == ["wait", "f"] for row in locks):
                        resource = "info\x1f" + table_names[table] + ":"
                        assert any(row[2].startswith(resource) and row[3:] == ["exclusive", "t"]
                                   for row in locks), locks
                        return
                time.sleep(0.01)
            raise AssertionError("waiter did not reach the held row lock")

        def overlap(key, waiter_command, returned, remaining, *, holder_changes=None,
                    isolation="READ COMMITTED", release="COMMIT", state=None):
            original = f"id={key}"
            if holder_changes is None:
                holder_changes = [(f"UPDATE {table} SET v=20 WHERE {original}", (key, 20))]
            if isolation:
                query(waiter, f"BEGIN ISOLATION LEVEL {isolation}", ready=b"T", tag="BEGIN")
            if isolation == "REPEATABLE READ":
                query(waiter, f"SELECT id,v FROM {table} WHERE {original}", [[str(key), "10"]], b"T")
            query(holder, "BEGIN ISOLATION LEVEL READ COMMITTED", ready=b"T", tag="BEGIN")
            for command, image in holder_changes:
                query(holder, command + " RETURNING id,v", rows([image]), b"T",
                      tag=command.split()[0] + " 1")
            sql = wire_sql(waiter_command + " RETURNING id,v")
            waiter.sendall(client.typed(b"Q", sql.encode() + b"\0"))
            wait_for_holder()
            assert not select.select([waiter], [], [], 0)[0], sql
            query(holder, release, tag=release)

            messages = client.read_until_ready(waiter)
            actual, status, message, names, tag, types = runner.decode_wire_result(messages, include_types=True)
            assert status == state and actual == rows(returned), (sql, status, message, actual)
            ready = b"E" if state else b"T" if isolation else b"I"
            assert messages[-1] == (b"Z", ready), (sql, messages[-1])
            kinds = [kind for kind, _ in messages]
            if state:
                assert kinds == [b"E", b"Z"] and tag is None, (sql, kinds, tag)
                error(waiter, "SELECT 1", "25P02")
                query(waiter, "ROLLBACK", tag="ROLLBACK")
            else:
                assert kinds == [b"T", *([b"D"] * len(actual)), b"C", b"Z"], (sql, kinds)
                assert names == ["id", "v"] and types == [23, 23], (sql, names, types)
                assert tag == f"{waiter_command.split()[0]} {len(returned)}", (sql, tag)
                if isolation:
                    query(waiter, "COMMIT", tag="COMMIT")
            query(holder, f"SELECT id,v FROM {table} WHERE cohort={key} ORDER BY id",
                  rows(remaining), tag=f"SELECT {len(remaining)}")
            survivors.update(remaining)

        def increment(condition):
            return f"UPDATE {table} SET v=v+1 WHERE {condition}"

        def delete(condition):
            return f"DELETE FROM {table} WHERE {condition}"

        overlap(1, increment("id=1"), [(1, 21)], [(1, 21)], isolation=None)
        overlap(2, increment("id=2"), [(2, 21)], [(2, 21)])
        overlap(3, increment("id=3"), [], [(103, 20)],
                holder_changes=[(f"UPDATE {table} SET id=103,v=20 WHERE id=3", (103, 20))])
        overlap(4, increment("cohort=4"), [(104, 21)], [(104, 21)],
                holder_changes=[(f"UPDATE {table} SET id=104,v=20 WHERE id=4", (104, 20))])
        overlap(5, increment("id=5 AND v=10"), [], [(5, 20)])
        overlap(6, delete("id=6"), [(6, 20)], [])
        overlap(7, delete("id=7 AND v=10"), [], [(7, 20)])
        overlap(8, increment("id=8"), [], [], holder_changes=[(delete("id=8"), (8, 10))])
        overlap(9, increment("id=9"), [], [(9, 20)], isolation="REPEATABLE READ", state="40001")
        overlap(10, increment("id=10"), [(10, 11)], [(10, 11)], release="ROLLBACK")
        overlap(11, increment("id=11"), [(11, 41)], [(11, 41)], holder_changes=[
            (f"UPDATE {table} SET v=20 WHERE id=11", (11, 20)),
            (f"UPDATE {table} SET v=40 WHERE id=11", (11, 40)),
        ])
        return survivors

    def check_survivors(sock, survivors):
        table = "lock_sp_versions"
        expected = [[str(key), str(value)] for key, value in sorted(survivors.items())]
        query(sock, f"SELECT id,v FROM {table} ORDER BY id", expected, tag=f"SELECT {len(expected)}")
        for key in sorted({*range(1, 12), *survivors}):
            matched = [[str(key), str(survivors[key])]] if key in survivors else []
            query(sock, f"SELECT id,v FROM {table} WHERE id={key}", matched, tag=f"SELECT {len(matched)}")

    def reopen_after_crash():
        server["process"].kill()
        assert server["process"].wait(timeout=10) == -signal.SIGKILL
        server["sock"].close()
        server["sock"] = None
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", server["dir"], "--server", str(server["port"]), "--insecure"],
            cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            try:
                runner.reconnect_ours(server, client)
                return
            except ConnectionRefusedError:
                assert server["process"].poll() is None and time.monotonic() < deadline
                time.sleep(0.05)

    try:
        second = socket.create_connection((host if reference else "127.0.0.1", server["port"]), timeout=120)
        if reference:
            second_pid, _ = client.startup_reference(second, user, database, password)
        else:
            second_pid, _ = client.startup(second, "alice", "info")
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
        first.settimeout(10)
        second.settimeout(10)
        survivors = overlapping_writes(first, second, second_pid)
        check_survivors(first, survivors)
        if not reference:
            second.close()
            second = None
            reopen_after_crash()
            check_survivors(server["sock"], survivors)
        print("[DML CONCURRENT ROW VERSIONS] 11 cases passed")
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
