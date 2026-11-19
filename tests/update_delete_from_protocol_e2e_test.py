#!/usr/bin/env python3
"""Source-driven UPDATE/DELETE binding, atomicity, and concurrent rechecks."""

import importlib.util
import select
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "source_dml_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    def decode(sql, messages, ready=b"I", log=True):
        statuses = [body for kind, body in messages if kind == b"Z"]
        decoded = runner.decode_wire_result(messages, include_types=True)
        assert statuses == [ready], (sql, statuses, decoded)
        if log:
            print("SOURCE_DML", sql, decoded, "READY", statuses, flush=True)
        return decoded

    def query(sql, connection=None, ready=b"I", log=True):
        return decode(sql, client.simple_query(connection or sock, sql), ready, log)

    def expect(sql, tag, rows=None, connection=None, ready=b"I"):
        result = query(sql, connection, ready, log=False)
        assert result[1] is None and result[4] == tag, (sql, result)
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)
        return result[0]

    def text_rows(values):
        return [[str(value) if value is not None else None for value in row] for row in values]

    def joined_conflicts(waiter, waiter_pid):
        target, source = "source_recheck_target", "source_recheck_source"
        expect(f"CREATE TABLE {target}(id INT PRIMARY KEY,k INT,v INT,cohort INT)", "CREATE TABLE")
        expect(f"CREATE TABLE {source}(id INT PRIMARY KEY,k INT,delta INT,cohort INT)", "CREATE TABLE")
        seed = ",".join(f"({n},10,10,{n})" for n in range(1, 11))
        expect(f"INSERT INTO {target} VALUES {seed},(999,999,999,999)", "INSERT 0 11")
        seed = ",".join(f"({10*n+1},10,1,{n}),({10*n+2},20,2,{n})" for n in range(1, 11))
        expect(f"INSERT INTO {source} VALUES {seed}", "INSERT 0 20")
        expect("SET lock_timeout=8000", "SET", connection=waiter)
        targets = [(999, 999, 999)]
        sources = []

        def wait_for_holder(statement):
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                assert not select.select([waiter], [], [], 0)[0], (statement, "waiter replied before holder released")
                if reference:
                    blockers = expect(f"SELECT pg_backend_pid()=ANY(pg_blocking_pids({waiter_pid}))",
                                      "SELECT 1", ready=b"T")
                    if blockers == [["t"]]:
                        return
                else:
                    locks, state, _, _, _, _ = query("SELECT * FROM pg_locks", ready=b"T", log=False)
                    assert state is None, state
                    if any(row[3:] == ["wait", "f"] for row in locks):
                        resource = f"info\x1f{target}:"
                        assert any(row[2].startswith(resource) and row[3:] == ["exclusive", "t"]
                                   for row in locks), locks
                        return
                time.sleep(0.01)
            raise AssertionError((statement, "waiter did not reach the held row lock"))

        def overlap(n, command, returned, remaining, *, predicate="t.k=s.k", holder_key=20,
                    source_steps=(), source_after=None, nullable=False, isolation="READ COMMITTED",
                    release="COMMIT", state=None):
            expect(f"BEGIN ISOLATION LEVEL {isolation}", "BEGIN", connection=waiter, ready=b"T")
            if isolation != "READ COMMITTED":
                expect(f"SELECT id,k,v FROM {target} WHERE id={n}", "SELECT 1",
                       text_rows([(n, 10, 10)]), waiter, b"T")
            expect("BEGIN ISOLATION LEVEL READ COMMITTED", "BEGIN", ready=b"T")
            for sql, tag in source_steps:
                expect(sql, tag, ready=b"T")
            expect(f"UPDATE {target} SET id={100+n},k={holder_key},v=20 WHERE id={n}", "UPDATE 1", ready=b"T")
            relation = f"{source} AS s"
            if nullable:
                relation += f" LEFT JOIN {source} AS missing ON missing.cohort=s.cohort AND missing.id=-1"
                predicate += " AND missing.delta IS NULL"
            if command == "UPDATE":
                statement = f"UPDATE {target} AS t SET v=t.v+s.delta FROM {relation}"
                projection = "new.id AS new_id,new.k AS new_key,new.v AS new_value,"
                columns = ["new_id", "new_key", "new_value"]
            else:
                statement = f"DELETE FROM {target} AS t USING {relation}"
                projection = ""
                columns = []
            statement += (f" WHERE t.cohort={n} AND t.cohort=s.cohort AND {predicate}"
                          " RETURNING old.id AS old_id,old.k AS old_key,old.v AS old_value,"
                          f"{projection}s.id AS source_id,s.k AS source_key,s.delta AS source_delta")
            columns = ["old_id", "old_key", "old_value", *columns, "source_id", "source_key", "source_delta"]
            if nullable:
                statement += ",missing.delta AS nullable_delta"
                columns.append("nullable_delta")
            waiter.sendall(client.typed(b"Q", statement.encode() + b"\0"))
            wait_for_holder(statement)
            assert not select.select([waiter], [], [], 0)[0], statement
            expect(release, release)
            messages = client.read_until_ready(waiter)
            rows, actual_state, message, names, tag, types = decode(statement, messages, b"E" if state else b"T")
            assert actual_state == state and rows == text_rows(returned), (statement, actual_state, message, rows)
            kinds = [kind for kind, _ in messages]
            if state:
                assert kinds == [b"E", b"Z"] and tag is None, (statement, kinds, tag)
                expect("ROLLBACK", "ROLLBACK", connection=waiter)
            else:
                assert kinds == [b"T", *([b"D"] * len(returned)), b"C", b"Z"], (statement, kinds)
                assert tag == f"{command} {len(returned)}" and names == columns and types == [23]*len(columns), (
                    statement, tag, names, types)
                expect("COMMIT", "COMMIT", connection=waiter)
            expect(f"SELECT id,k,v FROM {target} WHERE cohort={n} ORDER BY id", f"SELECT {len(remaining)}",
                   text_rows(remaining))
            targets.extend(remaining)
            if source_after is None:
                source_after = [(10*n+1, 10, 1), (10*n+2, 20, 2)]
            expect(f"SELECT id,k,delta FROM {source} WHERE cohort={n} ORDER BY id",
                   f"SELECT {len(source_after)}", text_rows(source_after))
            sources.extend(source_after)

        # A source that failed on the initial target cannot join a replacement.
        overlap(1, "UPDATE", [], [(101, 20, 20)])
        overlap(2, "DELETE", [], [(102, 20, 20)])
        # Both sources initially qualify. The replacement eliminates the first.
        overlap(3, "UPDATE", [(103, 20, 20, 103, 20, 22, 32, 20, 2)], [(103, 20, 22)],
                predicate="s.k>=t.k")
        overlap(4, "DELETE", [(104, 20, 20, 42, 20, 2)], [], predicate="s.k>=t.k")
        overlap(5, "UPDATE", [(105, 20, 20, 105, 20, 21, 51, 10, 1)], [(105, 20, 21)],
                predicate="s.k<=t.k")
        overlap(6, "DELETE", [(106, 10, 20, 61, 10, 1)], [], holder_key=10,
                source_steps=[(f"DELETE FROM {source} WHERE id=61", "DELETE 1"),
                              (f"INSERT INTO {source} VALUES(63,10,9,6)", "INSERT 0 1")],
                source_after=[(62, 20, 2), (63, 10, 9)])
        overlap(7, "UPDATE", [(7, 10, 10, 7, 10, 11, 71, 10, 1)], [(7, 10, 11)],
                release="ROLLBACK", source_steps=[(f"UPDATE {source} SET k=30,delta=9 WHERE id=71", "UPDATE 1")])
        overlap(8, "UPDATE", [], [(108, 20, 20)], isolation="REPEATABLE READ", state="40001")
        overlap(9, "DELETE", [], [(109, 20, 20)], isolation="SERIALIZABLE", state="40001")
        overlap(10, "UPDATE", [(110, 10, 20, 110, 10, 21, 101, 10, 1, None)], [(110, 10, 21)],
                holder_key=10, nullable=True,
                source_steps=[(f"UPDATE {source} SET k=30,delta=9 WHERE id=101", "UPDATE 1")],
                source_after=[(101, 30, 9), (102, 20, 2)])
        return sorted(targets), sorted(sources)

    def check_recheck_rows(targets, sources):
        expect("SELECT id,k,v FROM source_recheck_target ORDER BY id",
               f"SELECT {len(targets)}", text_rows(targets))
        expect("SELECT id,k,delta FROM source_recheck_source ORDER BY id",
               f"SELECT {len(sources)}", text_rows(sources))
        by_key = {row[0]: row for row in targets}
        for key in (*range(1, 11), *range(101, 111)):
            matched = [by_key[key]] if key in by_key else []
            expect(f"SELECT id,k,v FROM source_recheck_target WHERE id={key}",
                   f"SELECT {len(matched)}", text_rows(matched))

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
                return server["sock"]
            except ConnectionRefusedError:
                assert server["process"].poll() is None and time.monotonic() < deadline
                time.sleep(0.05)

    try:
        setup = [
            "CREATE TABLE source_dml_target "
            "(id INT PRIMARY KEY, val INT UNIQUE);",
            "CREATE TABLE source_dml_source (id INT, val INT);",
            "INSERT INTO source_dml_target VALUES (1, 10), (2, 20), (3, 30);",
            "INSERT INTO source_dml_source VALUES "
            "(1, 110), (1, 111), (2, 220);",
        ]
        for sql in setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)

        # A FROM/USING keyword with no valid source must never disappear
        # from the AST and turn a malformed statement into target-only DML.
        for malformed_sql in (
                "UPDATE source_dml_target SET val = id + 100 FROM;",
                "UPDATE source_dml_target SET val = id + 100 "
                "FROM source_dml_source JOIN;",
                "UPDATE source_dml_target dst SET val = dst.val + 100 "
                "FROM source_dml_source a JOIN source_dml_source b;",
                "DELETE FROM source_dml_target USING;",
                "DELETE FROM source_dml_target "
                "USING source_dml_source LEFT JOIN;",
                "DELETE FROM source_dml_target "
                "USING source_dml_source a JOIN source_dml_source b;",
                "DELETE FROM source_dml_target USING source_dml_source a "
                "JOIN source_dml_source b CROSS JOIN source_dml_source c;"):
            _, state, message, _, command_tag, _ = query(malformed_sql)
            assert state == "42601", (malformed_sql, state, message)
            assert command_tag is None, (malformed_sql, command_tag)
            rows, state, message, headers, command_tag, type_oids = query(
                "SELECT id, val FROM source_dml_target ORDER BY id;")
            assert state is None, (state, message)
            assert rows == [["1", "10"], ["2", "20"], ["3", "30"]], rows
            assert headers == ["id", "val"] and type_oids == [23, 23], (
                headers, type_oids)
            assert command_tag == "SELECT 3", command_tag

        rows, state, message, headers, command_tag, type_oids = query(
            "UPDATE source_dml_target AS dst SET val = src.val "
            "FROM source_dml_source AS src WHERE dst.id = src.id "
            "RETURNING id, val;")
        assert state == "42702" and command_tag is None, (state, message, command_tag)
        rows, state, message, headers, command_tag, type_oids = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None and rows == [["1", "10"], ["2", "20"], ["3", "30"]], (rows, state)
        assert headers == ["id", "val"] and type_oids == [23, 23], (headers, type_oids)
        rows, state, message, headers, command_tag, type_oids = query(
            "UPDATE source_dml_target AS dst SET val = src.val "
            "FROM source_dml_source AS src WHERE dst.id = src.id "
            "RETURNING dst.id, dst.val;")
        assert state is None, (state, message)
        assert command_tag == "UPDATE 2", command_tag
        assert headers == ["id", "val"] and type_oids == [23, 23], (
            headers, type_oids)
        assert len(rows) == 2, rows
        values = {row[0]: row[1] for row in rows}
        assert values["1"] in {"110", "111"}, values
        assert values["2"] == "220", values

        rows, state, message, _, command_tag, _ = query(
            "DELETE FROM source_dml_target AS dst "
            "USING source_dml_source AS src "
            "WHERE dst.id = src.id AND dst.id = 2 RETURNING id, val;")
        assert state == "42702" and command_tag is None, (state, message, command_tag)
        unchanged, state, message, headers, _, type_oids = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None and unchanged == [["1", values["1"]], ["2", "220"], ["3", "30"]], (unchanged, state)
        assert headers == ["id", "val"] and type_oids == [23, 23], (headers, type_oids)
        rows, state, message, _, command_tag, _ = query(
            "DELETE FROM source_dml_target AS dst "
            "USING source_dml_source AS src "
            "WHERE dst.id = src.id AND dst.id = 2 RETURNING dst.id, dst.val;")
        assert state is None, (state, message)
        assert rows == [["2", "220"]], rows
        assert command_tag == "DELETE 1", command_tag

        # The original legal LEFT source now consumes its genuine typed plan.
        _, state, _, _, _, _ = query(
            "UPDATE source_dml_target AS dst SET val = src.val "
            "FROM source_dml_source AS src LEFT JOIN source_dml_source AS extra "
            "ON src.id = extra.id WHERE dst.id = src.id;")
        assert state is None, state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None, (state, message)
        assert len(rows) == 2 and rows[-1] == ["3", "30"], rows

        # Cursor-positioned mutations are recognized, rejected explicitly,
        # and leave both the target and the connection usable.
        _, state, _, _, _, _ = query(
            "UPDATE source_dml_target SET val = 999 "
            "WHERE CURRENT OF missing_cursor;")
        # Missing named cursors have 34000 in PostgreSQL. The current project
        # cursor-positioned mutation boundary is still explicitly unsupported.
        assert state == ("34000" if reference else "0A000"), state
        _, state, _, _, _, _ = query(
            "DELETE FROM source_dml_target WHERE CURRENT OF missing_cursor;")
        assert state == ("34000" if reference else "0A000"), state
        rows, state, message, _, command_tag, _ = query(
            "SELECT id, val FROM source_dml_target ORDER BY id;")
        assert state is None, (state, message)
        assert len(rows) == 2 and command_tag == "SELECT 2", (rows, command_tag)

        atomic_setup = [
            "CREATE TABLE source_atomic_target "
            "(id INT PRIMARY KEY, val INT UNIQUE);",
            "CREATE TABLE source_atomic_source (id INT, val INT);",
            "INSERT INTO source_atomic_target VALUES "
            "(1, 10), (2, 20), (3, 40);",
            "INSERT INTO source_atomic_source VALUES (1, 30), (2, 40);",
        ]
        for sql in atomic_setup:
            _, state, message, _, _, _ = query(sql)
            assert state is None, (sql, state, message)
        _, state, _, _, _, _ = query(
            "UPDATE source_atomic_target AS dst SET val = src.val "
            "FROM source_atomic_source AS src WHERE dst.id = src.id;")
        assert state == "23505", state
        rows, state, message, _, _, _ = query(
            "SELECT id, val FROM source_atomic_target ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "10"], ["2", "20"], ["3", "40"]], rows

        # A final successful statement proves protocol recovery after errors.
        _, state, message, _, command_tag, _ = query(
            "UPDATE source_atomic_target SET val = 50 WHERE id = 1;")
        assert state is None, (state, message)
        assert command_tag == "UPDATE 1", command_tag

        sock.settimeout(10)
        address = (host, port) if reference else ("127.0.0.1", server["port"])
        with socket.create_connection(address, timeout=10) as waiter:
            if reference:
                waiter_pid, _ = client.startup_reference(waiter, user, database, password)
            else:
                waiter_pid, _ = client.startup(waiter, "alice", "info")
            expected_targets, expected_sources = joined_conflicts(waiter, waiter_pid)
        check_recheck_rows(expected_targets, expected_sources)
        if not reference:
            sock = reopen_after_crash()
            check_recheck_rows(expected_targets, expected_sources)
        print("[UPDATE/DELETE SOURCE RECHECK] 10 conflicts passed")
        print("[UPDATE/DELETE FROM PROTOCOL E2E] passed")
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, "ROLLBACK")
                for table in ("source_dml_target", "source_dml_source", "source_atomic_target", "source_atomic_source",
                              "source_recheck_target", "source_recheck_source"):
                    client.simple_query(sock, "DROP TABLE IF EXISTS " + table + ";")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
