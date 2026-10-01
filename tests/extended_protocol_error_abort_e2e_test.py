#!/usr/bin/env python3
"""Extended-query errors abort before ErrorResponse, not after Sync/Execute."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "extended_abort_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        first = server["sock"]
    second = None
    table = "extended_abort_" + uuid.uuid4().hex
    created = False
    waiting_for_sync = False

    def query(sock, sql, rows=None, state=None, ready=b"I"):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    def parse(name, sql, oids=()):
        return client.typed(b"P", name.encode() + b"\0" + sql.encode() + b"\0" +
                            struct.pack("!H", len(oids)) +
                            b"".join(struct.pack("!I", oid) for oid in oids))

    def bind(name, values=()):
        return client.typed(b"B", b"\0" + name.encode() + b"\0" +
                            struct.pack("!HH", 0, len(values)) +
                            b"".join(struct.pack("!I", len(value)) + value for value in values) +
                            struct.pack("!H", 0))

    def completed(packet, expected):
        first.sendall(packet + client.typed(b"H", b""))
        assert client.read_message(first) == (expected, b"")

    try:
        second = socket.create_connection(
            (host if reference else "127.0.0.1", server["port"]), timeout=120)
        if reference:
            client.startup_reference(second, user, database, password)
        else:
            client.startup(second, "alice", "info")
        query(first, f"CREATE TABLE {table}(id INT PRIMARY KEY);")
        created = True
        query(first, f"INSERT INTO {table} VALUES(1),(2);")
        for failure in ("bind_integer", "parse_multiple", "parse_duplicate",
                        "bind_missing", "bind_count", "execute_missing", "describe_missing"):
            for savepoint in (False, True):
                key = int(uuid.uuid4().hex[:8], 16)
                name = "prepared_" + uuid.uuid4().hex
                query(first, "BEGIN;", ready=b"T")
                query(first, f"INSERT INTO {table} VALUES(10);", ready=b"T")
                query(first, f"SELECT id FROM {table} WHERE id=1 FOR UPDATE;", [["1"]], ready=b"T")
                query(first, f"SELECT pg_advisory_xact_lock({key});", ready=b"T")
                if savepoint:
                    query(first, "SAVEPOINT recover;", ready=b"T")
                query(first, f"INSERT INTO {table} VALUES(20);", ready=b"T")
                query(first, f"SELECT id FROM {table} WHERE id=2 FOR UPDATE;", [["2"]], ready=b"T")
                query(first, f"SELECT pg_advisory_xact_lock({key + 1});", ready=b"T")
                query(first, f"SELECT pg_advisory_lock({key + 2});", ready=b"T")
                if failure in ("bind_integer", "bind_count", "parse_duplicate"):
                    completed(parse(name, "SELECT $1;", (23,)), b"1")
                if failure == "bind_integer":
                    packet, state = bind(name, (b"abc",)), "22P02"
                elif failure == "parse_multiple":
                    packet, state = parse(name, "SELECT 1; SELECT 2;"), "42601"
                elif failure == "parse_duplicate":
                    packet, state = parse(name, "SELECT 1;"), "42P05"
                elif failure == "bind_missing":
                    packet, state = bind(name), "26000"
                elif failure == "bind_count":
                    packet, state = bind(name), "08P01"
                elif failure == "execute_missing":
                    packet, state = client.typed(b"E", name.encode() + b"\0" + struct.pack("!I", 0)), "34000"
                else:
                    packet, state = client.typed(b"D", b"S" + name.encode() + b"\0"), "26000"
                first.sendall(packet + client.typed(b"H", b""))
                waiting_for_sync = True
                error = client.read_message(first)
                assert error[0] == b"E" and runner.decode_wire_result([error])[1] == state, (failure, error)
                # The failed connection has not received Sync or recovery.
                query(second, "BEGIN;", ready=b"T")
                query(second, "SET lock_timeout=50;", ready=b"T")
                query(second, f"SELECT id FROM {table} WHERE id=2 FOR UPDATE;", [["2"]], ready=b"T")
                query(second, f"SELECT pg_try_advisory_xact_lock({key + 1});", [["t"]], ready=b"T")
                query(second, f"SELECT pg_try_advisory_xact_lock({key});", [["f" if savepoint else "t"]], ready=b"T")
                query(second, f"SELECT pg_try_advisory_lock({key + 2});", [["f"]], ready=b"T")
                query(second, f"SELECT id FROM {table} WHERE id=1 FOR UPDATE;",
                      [] if savepoint else [["1"]], "55P03" if savepoint else None,
                      b"E" if savepoint else b"T")
                query(second, "ROLLBACK;")
                first.sendall(client.typed(b"S", b""))
                assert client.read_until_ready(first)[-1] == (b"Z", b"E")
                waiting_for_sync = False
                query(first, "SELECT 1;", [], "25P02", b"E")
                if savepoint:
                    query(first, "ROLLBACK TO recover;", ready=b"T")
                    query(first, f"SELECT id FROM {table} ORDER BY id;", [["1"], ["2"], ["10"]], ready=b"T")
                query(first, "ROLLBACK;")
                query(first, f"SELECT pg_advisory_unlock({key + 2});", [["t"]])
                query(first, f"SELECT id FROM {table} ORDER BY id;", [["1"], ["2"]])
                # Close is legal for a missing statement too; no named
                # statement is left behind on the reference connection.
                completed(client.typed(b"C", b"S" + name.encode() + b"\0"), b"3")
                first.sendall(client.typed(b"S", b""))
                assert client.read_until_ready(first)[-1] == (b"Z", b"I")
        # Objects created in the parent survive an error in the child;
        # reuse requires ROLLBACK TO, not merely Sync after the error.
        name = "preserved_" + uuid.uuid4().hex
        portal = "portal_" + uuid.uuid4().hex
        query(first, "BEGIN;", ready=b"T")
        completed(parse(name, "SELECT 21;"), b"1")
        query(first, "SAVEPOINT parse_recover;", ready=b"T")
        first.sendall(parse(name, "SELECT 22;") + client.typed(b"H", b""))
        waiting_for_sync = True
        error = client.read_message(first)
        assert error[0] == b"E" and runner.decode_wire_result([error])[1] == "42P05", error
        first.sendall(client.typed(b"S", b""))
        assert client.read_until_ready(first)[-1] == (b"Z", b"E")
        waiting_for_sync = False
        query(first, "ROLLBACK TO parse_recover;", ready=b"T")
        named_bind = client.typed(b"B", portal.encode() + b"\0" + name.encode() + b"\0" +
                                 struct.pack("!HHH", 0, 0, 0))
        completed(named_bind, b"2")
        query(first, "SAVEPOINT bind_recover;", ready=b"T")
        first.sendall(named_bind + client.typed(b"H", b""))
        waiting_for_sync = True
        error = client.read_message(first)
        assert error[0] == b"E" and runner.decode_wire_result([error])[1] == "42P03", error
        first.sendall(client.typed(b"S", b""))
        assert client.read_until_ready(first)[-1] == (b"Z", b"E")
        waiting_for_sync = False
        query(first, "ROLLBACK TO bind_recover;", ready=b"T")
        first.sendall(client.typed(b"E", portal.encode() + b"\0" + struct.pack("!I", 0)) +
                      client.typed(b"S", b""))
        messages = client.read_until_ready(first)
        assert runner.decode_wire_result(messages)[0:2] == ([["21"]], None), messages
        assert messages[-1] == (b"Z", b"T"), messages
        query(first, "ROLLBACK;")
        completed(client.typed(b"C", b"S" + name.encode() + b"\0"), b"3")
        first.sendall(client.typed(b"S", b""))
        assert client.read_until_ready(first)[-1] == (b"Z", b"I")
        # A Flush-only Execute can already hold an implicit transaction's
        # locks. A later Bind error must abort it before the next Sync.
        key = int(uuid.uuid4().hex[:8], 16)
        name = "implicit_" + uuid.uuid4().hex
        completed(parse(name, "SELECT $1;", (23,)), b"1")
        first.sendall(client.typed(b"S", b""))
        assert client.read_until_ready(first)[-1] == (b"Z", b"I")
        for sql in (f"SELECT pg_advisory_xact_lock({key});",
                    f"SELECT pg_advisory_lock({key + 1});"):
            first.sendall(parse("", sql) + bind("") +
                          client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                          client.typed(b"H", b""))
            messages = []
            while not messages or messages[-1][0] != b"C":
                messages.append(client.read_message(first))
                assert messages[-1][0] != b"E", messages
            assert runner.decode_wire_result(messages)[1] is None, messages
        first.sendall(bind(name, (b"abc",)) + client.typed(b"H", b""))
        waiting_for_sync = True
        error = client.read_message(first)
        assert error[0] == b"E" and runner.decode_wire_result([error])[1] == "22P02", error
        query(second, f"SELECT pg_try_advisory_xact_lock({key});", [["t"]])
        query(second, f"SELECT pg_try_advisory_lock({key + 1});", [["f"]])
        first.sendall(client.typed(b"S", b""))
        assert client.read_until_ready(first)[-1] == (b"Z", b"I")
        waiting_for_sync = False
        query(first, f"SELECT pg_advisory_unlock({key + 1});", [["t"]])
        completed(client.typed(b"C", b"S" + name.encode() + b"\0"), b"3")
        first.sendall(client.typed(b"S", b""))
        assert client.read_until_ready(first)[-1] == (b"Z", b"I")
        query(first, f"DROP TABLE {table};")
        created = False
        print("[EXTENDED PROTOCOL ERROR ABORT " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if second is not None:
            try:
                client.simple_query(second, "ROLLBACK;")
            finally:
                second.close()
        if reference:
            try:
                if waiting_for_sync:
                    first.sendall(client.typed(b"S", b""))
                    client.read_until_ready(first)
                client.simple_query(first, "ROLLBACK;")
                if created:
                    client.simple_query(first, f"DROP TABLE {table};")
            finally:
                first.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
