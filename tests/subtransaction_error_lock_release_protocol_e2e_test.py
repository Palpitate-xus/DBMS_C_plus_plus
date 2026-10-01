#!/usr/bin/env python3
"""An error releases post-savepoint locks before client ROLLBACK TO arrives."""

import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "subtransaction_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    peers = []
    prefix = "abort_sp_" + uuid.uuid4().hex
    target, prior = prefix + "_target", prefix + "_prior"
    created = []
    key = int(uuid.uuid4().hex[:8], 16)

    def query(sock, sql, rows=None, state=None, ready=b"I"):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    try:
        first = server["sock"]
        for _ in range(2):
            peer = socket.create_connection((host if reference else "127.0.0.1", server["port"]), timeout=120)
            peers.append(peer)
            if reference:
                client.startup_reference(peer, user, database, password)
            else:
                client.startup(peer, "alice", "info")
        second, third = peers
        for table in (target, prior):
            query(first, f"CREATE TABLE {table}(id INT PRIMARY KEY,v INT);")
            created.append(table)
        query(first, f"INSERT INTO {target} VALUES(1,1),(2,2);")
        query(first, f"INSERT INTO {prior} VALUES(1,1);")
        query(first, "BEGIN;", ready=b"T")
        query(first, "SET lock_timeout=50;", ready=b"T")
        query(first, f"SELECT id FROM {target} WHERE id=1 FOR UPDATE;", [["1"]], ready=b"T")
        query(second, "BEGIN;", ready=b"T")
        query(second, "SET lock_timeout=50;", ready=b"T")
        query(second, f"SELECT id FROM {prior} WHERE id=1 FOR UPDATE;", [["1"]], ready=b"T")
        query(second, f"SELECT pg_advisory_xact_lock({key});", ready=b"T")
        query(second, "SAVEPOINT recover;", ready=b"T")
        query(second, f"SELECT id FROM {target} WHERE id=2 FOR UPDATE;", [["2"]], ready=b"T")
        query(second, f"SELECT pg_advisory_xact_lock({key + 1});", ready=b"T")
        query(second, f"SELECT pg_advisory_lock({key + 3});", ready=b"T")
        query(second, f"UPDATE {target} SET v=999 WHERE id=1;", [], "55P03", b"E")
        # No recovery command has been sent by the failed backend yet.
        query(first, f"SELECT id FROM {target} WHERE id=2 FOR UPDATE;", [["2"]], ready=b"T")
        query(third, "BEGIN;", ready=b"T")
        query(third, "SET lock_timeout=50;", ready=b"T")
        query(third, f"SELECT pg_try_advisory_xact_lock({key + 1});", [["t"]], ready=b"T")
        query(third, f"SELECT pg_try_advisory_xact_lock({key});", [["f"]], ready=b"T")
        query(third, f"SELECT pg_try_advisory_lock({key + 3});", [["f"]], ready=b"T")
        query(third, f"SELECT pg_advisory_lock({key + 2});", ready=b"T")
        query(third, f"SELECT id FROM {prior} WHERE id=1 FOR UPDATE;", [], "55P03", b"E")
        # With no user savepoint, the top-level error releases transaction
        # locks too, although ReadyForQuery remains E until recovery.
        query(first, f"SELECT pg_try_advisory_xact_lock({key + 1});", [["t"]], ready=b"T")
        query(first, f"SELECT pg_try_advisory_lock({key + 2});", [["f"]], ready=b"T")
        query(third, "ROLLBACK;")
        query(third, f"SELECT pg_advisory_unlock({key + 2});", [["t"]])
        query(second, "SELECT 1;", [], "25P02", b"E")
        query(second, "ROLLBACK TO SAVEPOINT recover;", ready=b"T")
        query(second, f"SELECT id,v FROM {prior};", [["1", "1"]], ready=b"T")
        query(second, "COMMIT;")
        query(second, f"SELECT pg_advisory_unlock({key + 3});", [["t"]])
        query(first, "ROLLBACK;")
        query(third, "BEGIN;", ready=b"T")
        query(third, f"SELECT id FROM {prior} WHERE id=1 FOR UPDATE;", [["1"]], ready=b"T")
        query(third, f"SELECT pg_try_advisory_xact_lock({key});", [["t"]], ready=b"T")
        query(third, "ROLLBACK;")
        query(first, f"SELECT id,v FROM {target} ORDER BY id;", [["1", "1"], ["2", "2"]])
        print("[SUBTRANSACTION ERROR LOCK RELEASE " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        for peer in peers:
            try:
                client.simple_query(peer, "ROLLBACK;")
            except Exception:
                pass
            peer.close()
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
