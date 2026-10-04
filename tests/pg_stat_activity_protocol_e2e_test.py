#!/usr/bin/env python3
"""pg_stat_activity projections are typed and query text is permission-checked."""

import importlib.util
from pathlib import Path
import socket
import struct
import threading
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "activity_query_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    admin = server["sock"]
    victim = monitor = None
    worker_error = []
    worker = None

    def query(sock, sql, extended=False, state=None, headers=None, types=None):
        if extended:
            parse = client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
            sock.sendall(parse +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result)
        assert messages[-1][0] == b"Z" and messages[-1][1] in (b"I", b"T"), (
            sql, messages[-1])
        if headers is not None:
            assert result[3] == headers and result[5] == types, (sql, result)
        return result

    def connect_as(user, password):
        sock = socket.create_connection(("127.0.0.1", server["port"]), timeout=15)
        sock.settimeout(15)
        client.startup(sock, user, "info", password=password)
        return sock

    try:
        # The test runner starts with an Alice-only HBA rule. New connections
        # reread this file, so allow the two test roles to authenticate.
        Path(server["dir"], "pg_hba.conf").write_text(
            "host all all 127.0.0.1/32 scram-sha-256\n", encoding="utf-8")
        for sql in (
                "CREATE USER act_victim WITH PASSWORD 'VictimPass9!'",
                "CREATE USER act_monitor WITH PASSWORD 'MonitorPass9!'",
                "CREATE TABLE activity_secret (id INTEGER PRIMARY KEY, secret TEXT)",
                "INSERT INTO activity_secret VALUES (1, 'classified')",
                "GRANT SELECT ON activity_secret TO act_victim",
                "GRANT UPDATE ON activity_secret TO act_victim"):
            result = query(admin, sql)
            assert result[1] is None, (sql, result)

        victim = connect_as("act_victim", "VictimPass9!")
        monitor = connect_as("act_monitor", "MonitorPass9!")

        # Both Simple Query and Extended Query must describe the actual subset
        # with PostgreSQL type OIDs, rather than five all-text columns.
        for extended in (False, True):
            result = query(
                monitor,
                "SELECT pid, datname, usename, state, query "
                "FROM pg_catalog.pg_stat_activity WHERE pid > 0 LIMIT 1",
                extended, headers=["pid", "datname", "usename", "state", "query"],
                types=[23, 19, 19, 25, 25])
            assert result[1] is None and result[0], result
            result = query(
                monitor,
                "SELECT usename FROM pg_stat_activity WHERE usename = 'act_monitor'",
                extended, headers=["usename"], types=[19])
            assert result[0] == [["act_monitor"]], result

        for sql, state in (
                ("SELECT * FROM pg_catalog.pg_stat_activity", "0A000"),
                ("SELECT query_id FROM pg_catalog.pg_stat_activity", "0A000"),
                ("SELECT missing_column FROM pg_catalog.pg_stat_activity", "42703"),
                ("SELECT pid FROM pg_catalog.pg_stat_activity WHERE pid", "42804")):
            result = query(monitor, sql, state=state)
            assert result[2], (sql, result)

        # Hold a row lock as Alice and park the victim on an UPDATE carrying a
        # unique SQL marker. An unrelated ordinary role may see the session,
        # but not its SQL text.
        result = query(admin, "BEGIN")
        assert result[1] is None, result
        result = query(admin, "SELECT id FROM activity_secret WHERE id = 1 FOR UPDATE")
        assert result[0] == [["1"]], result
        private_sql = (
            "UPDATE /*PRIVATE_ACTIVITY_MARKER*/ activity_secret "
            "SET id = id WHERE id = 1")

        def block_victim():
            try:
                result = query(victim, private_sql)
                if result[1] is not None:
                    worker_error.append(result)
            except BaseException as error:  # surfaced in the main test thread
                worker_error.append(error)

        worker = threading.Thread(target=block_victim, daemon=True)
        worker.start()
        deadline = time.monotonic() + 10
        hidden_row = None
        while time.monotonic() < deadline:
            result = query(
                monitor,
                "SELECT state, query FROM pg_catalog.pg_stat_activity "
                "WHERE usename = 'act_victim'")
            if result[0] and result[0][0][0] == "active":
                hidden_row = result[0][0]
                break
            if worker_error:
                raise AssertionError("victim UPDATE failed", worker_error[0])
            time.sleep(0.03)
        assert hidden_row is not None, "victim UPDATE did not become active"
        assert hidden_row[1] is None, hidden_row

        visible = query(
            admin,
            "SELECT query FROM pg_catalog.pg_stat_activity "
            "WHERE usename = 'act_victim'")
        assert visible[0] and "PRIVATE_ACTIVITY_MARKER" in visible[0][0][0], visible

        # Membership in PostgreSQL's monitoring role grants visibility across
        # users. This also exercises inherited role membership resolution.
        for sql in (
                "CREATE ROLE pg_read_all_stats",
                "GRANT pg_read_all_stats TO act_monitor"):
            result = query(admin, sql)
            assert result[1] is None, (sql, result)
        visible = query(
            monitor,
            "SELECT query FROM pg_catalog.pg_stat_activity "
            "WHERE usename = 'act_victim'")
        assert visible[0] and "PRIVATE_ACTIVITY_MARKER" in visible[0][0][0], visible

        # Unqualified lookup must keep resolving a user relation when one
        # shadows the virtual view; the explicitly-qualified catalog remains
        # available.
        for sql in (
                "CREATE TABLE pg_stat_activity (id INTEGER)",
                "INSERT INTO pg_stat_activity VALUES (23)"):
            result = query(admin, sql)
            assert result[1] is None, (sql, result)
        result = query(admin, "SELECT * FROM pg_stat_activity",
                       headers=["id"], types=[23])
        assert result[0] == [["23"]], result
        result = query(admin, "SELECT pid FROM pg_catalog.pg_stat_activity LIMIT 1",
                       headers=["pid"], types=[23])
        assert result[1] is None, result
        print("[PG STAT ACTIVITY PROTOCOL E2E] typed subset and privacy passed")
    finally:
        if admin is not None:
            try:
                client.simple_query(admin, "ROLLBACK")
            except Exception:
                pass
        if worker is not None:
            worker.join(timeout=5)
        if victim is not None:
            victim.close()
        if monitor is not None:
            monitor.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
