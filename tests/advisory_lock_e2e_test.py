#!/usr/bin/env python3
"""Protocol regression for PostgreSQL-compatible advisory lock ownership."""

import concurrent.futures
import importlib.util
from pathlib import Path
import socket
import time


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "advisory_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def connect(database="info"):
        sock = socket.create_connection(
            ("127.0.0.1", server["port"]), timeout=15)
        sock.settimeout(15)
        client.startup(sock, "alice", database)
        return sock

    def query(sock, sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    def expect(sock, sql, rows, type_oids=None):
        actual, state, message, _, tag, actual_types = query(sock, sql)
        assert state is None, (sql, state, message)
        assert actual == rows, (sql, actual, rows)
        if type_oids is not None:
            assert actual_types == type_oids, (sql, actual_types, type_oids)
        if rows:
            assert tag == "SELECT 1", (sql, tag)
        return actual

    first = server["sock"]
    second = connect()
    third = connect()
    scoped = None
    try:
        # Signed bigint keys are owner-aware and reentrant. Void and boolean
        # functions retain their PostgreSQL wire types and NULL representation.
        expect(first, "SELECT pg_advisory_lock(-1);", [[""]], [2278])
        expect(first, "SELECT pg_advisory_lock(-1);", [[""]], [2278])
        expect(second, "SELECT pg_try_advisory_lock(-1);", [["f"]], [16])
        expect(first, "SELECT pg_advisory_unlock(-1);", [["t"]], [16])
        expect(second, "SELECT pg_try_advisory_lock(-1);", [["f"]], [16])
        expect(first, "SELECT pg_advisory_unlock(-1);", [["t"]], [16])
        expect(second, "SELECT pg_try_advisory_lock(-1);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(-1);", [["t"]], [16])
        expect(first, "SELECT pg_advisory_unlock(-1);", [["f"]], [16])
        expect(first, "SELECT pg_advisory_lock(NULL);", [[None]], [2278])
        rows, state, message, headers, _, types = query(
            first, 'SELECT pg_catalog.pg_try_advisory_lock(42) AS "got lock";')
        assert state is None and rows == [["t"]], (state, message, rows)
        assert headers == ["got lock"] and types == [16], (headers, types)
        expect(first, "SELECT pg_advisory_unlock(42);", [["t"]], [16])
        expect(second, "SELECT pg_try_advisory_lock(-1);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(-1);", [["t"]], [16])

        # The bigint and two-int overloads have distinct namespaces.
        expect(first, "SELECT pg_advisory_lock(4294967298);", [[""]], [2278])
        expect(second, "SELECT pg_try_advisory_lock(1, 2);", [["t"]], [16])
        expect(third, "SELECT pg_try_advisory_lock(1, 2);", [["f"]], [16])
        expect(second, "SELECT pg_advisory_unlock(1, 2);", [["t"]], [16])
        expect(first, "SELECT pg_advisory_unlock(4294967298);", [["t"]], [16])

        # Shared holders coexist. Exclusive requests conflict until all shared
        # counts are released, including multiple acquisitions by one owner.
        expect(first, "SELECT pg_advisory_lock_shared(50);", [[""]], [2278])
        expect(first, "SELECT pg_advisory_lock_shared(50);", [[""]], [2278])
        expect(second, "SELECT pg_try_advisory_lock_shared(50);", [["t"]], [16])
        expect(third, "SELECT pg_try_advisory_lock(50);", [["f"]], [16])
        expect(first, "SELECT pg_advisory_unlock_shared(50);", [["t"]], [16])
        expect(first, "SELECT pg_advisory_unlock_shared(50);", [["t"]], [16])
        expect(third, "SELECT pg_try_advisory_lock(50);", [["f"]], [16])
        expect(second, "SELECT pg_advisory_unlock_shared(50);", [["t"]], [16])
        expect(third, "SELECT pg_try_advisory_lock(50);", [["t"]], [16])
        expect(third, "SELECT pg_advisory_unlock(50);", [["t"]], [16])

        # A blocking request wakes after the conflicting owner releases.
        expect(first, "SELECT pg_advisory_lock(60);", [[""]], [2278])
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            waiter = pool.submit(query, third, "SELECT pg_advisory_lock(60);")
            time.sleep(0.1)
            assert not waiter.done(), "blocking advisory lock returned early"
            expect(first, "SELECT pg_advisory_unlock(60);", [["t"]], [16])
            rows, state, message, _, _, types = waiter.result(timeout=5)
            assert state is None and rows == [[""]] and types == [2278], (
                state, message, rows, types)
        expect(third, "SELECT pg_advisory_unlock(60);", [["t"]], [16])

        # Transaction locks follow savepoint, commit and rollback lifetimes;
        # session locks deliberately survive rollback.
        assert query(first, "BEGIN;")[1] is None
        expect(first, "SELECT pg_advisory_xact_lock(70);", [[""]], [2278])
        assert query(first, "SAVEPOINT advisory_sp;")[1] is None
        expect(first, "SELECT pg_advisory_xact_lock(71);", [[""]], [2278])
        expect(first, "SELECT pg_advisory_lock(72);", [[""]], [2278])
        expect(second, "SELECT pg_try_advisory_lock(70);", [["f"]], [16])
        expect(second, "SELECT pg_try_advisory_lock(71);", [["f"]], [16])
        assert query(first, "ROLLBACK TO SAVEPOINT advisory_sp;")[1] is None
        expect(second, "SELECT pg_try_advisory_lock(71);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(71);", [["t"]], [16])
        expect(second, "SELECT pg_try_advisory_lock(70);", [["f"]], [16])
        assert query(first, "ROLLBACK;")[1] is None
        expect(second, "SELECT pg_try_advisory_lock(70);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(70);", [["t"]], [16])
        expect(second, "SELECT pg_try_advisory_lock(72);", [["f"]], [16])
        expect(first, "SELECT pg_advisory_unlock(72);", [["t"]], [16])

        # Outside BEGIN, xact functions live for their one statement only.
        expect(first, "SELECT pg_advisory_xact_lock(73);", [[""]], [2278])
        expect(second, "SELECT pg_try_advisory_lock(73);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(73);", [["t"]], [16])

        # Shared transaction variants coexist and both release at their own
        # transaction boundaries.
        assert query(first, "BEGIN;")[1] is None
        assert query(second, "BEGIN;")[1] is None
        expect(first, "SELECT pg_advisory_xact_lock_shared(74);", [[""]], [2278])
        expect(second, "SELECT pg_try_advisory_xact_lock_shared(74);", [["t"]], [16])
        expect(third, "SELECT pg_try_advisory_lock(74);", [["f"]], [16])
        assert query(first, "COMMIT;")[1] is None
        expect(third, "SELECT pg_try_advisory_lock(74);", [["f"]], [16])
        assert query(second, "ROLLBACK;")[1] is None
        expect(third, "SELECT pg_try_advisory_lock(74);", [["t"]], [16])
        expect(third, "SELECT pg_advisory_unlock(74);", [["t"]], [16])

        # PREPARE transfers transaction ownership until COMMIT PREPARED.
        assert query(first, "BEGIN;")[1] is None
        expect(first, "SELECT pg_advisory_xact_lock(80);", [[""]], [2278])
        assert query(first, "PREPARE TRANSACTION 'advisory-lock-e2e';")[1] is None
        expect(second, "SELECT pg_try_advisory_lock(80);", [["f"]], [16])
        assert query(second, "COMMIT PREPARED 'advisory-lock-e2e';")[1] is None
        expect(second, "SELECT pg_try_advisory_lock(80);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(80);", [["t"]], [16])

        # Session cleanup paths release locks without disturbing other owners.
        expect(first, "SELECT pg_advisory_lock(89);", [[""]], [2278])
        expect(first, "SELECT pg_advisory_unlock_all();", [[""]], [2278])
        expect(second, "SELECT pg_try_advisory_lock(89);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(89);", [["t"]], [16])
        expect(first, "SELECT pg_advisory_lock(90);", [[""]], [2278])
        assert query(first, "DISCARD ALL;")[1] is None
        expect(second, "SELECT pg_try_advisory_lock(90);", [["t"]], [16])
        expect(second, "SELECT pg_advisory_unlock(90);", [["t"]], [16])

        expect(first, "SELECT pg_advisory_lock(91);", [[""]], [2278])
        first.close()
        server["sock"] = second
        deadline = time.time() + 5
        while True:
            rows, state, message, _, _, _ = query(
                second, "SELECT pg_try_advisory_lock(91);")
            assert state is None, (state, message)
            if rows == [["t"]]:
                break
            if time.time() >= deadline:
                raise AssertionError("disconnect did not release advisory lock")
            time.sleep(0.05)
        expect(second, "SELECT pg_advisory_unlock(91);", [["t"]], [16])

        # Same numeric key in another database is independent.
        assert query(second, "CREATE DATABASE advisory_scope;")[1] is None
        expect(second, "SELECT pg_advisory_lock(100);", [[""]], [2278])
        scoped = connect("advisory_scope")
        expect(scoped, "SELECT pg_try_advisory_lock(100);", [["t"]], [16])
        expect(scoped, "SELECT pg_advisory_unlock(100);", [["t"]], [16])
        scoped.close()
        scoped = None
        expect(second, "SELECT pg_advisory_unlock(100);", [["t"]], [16])
        assert query(second, "DROP DATABASE advisory_scope;")[1] is None

        # Integer-pair overflow is rejected before touching the lock table.
        _, state, _, _, _, _ = query(
            second, "SELECT pg_try_advisory_lock(2147483648, 0);")
        assert state == "22003", state
        print("[ADVISORY LOCK E2E] passed")
    finally:
        for sock in (scoped, third, second):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
