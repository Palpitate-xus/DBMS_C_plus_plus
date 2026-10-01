#!/usr/bin/env python3
"""COPY releases aborted transaction locks before CopyDone/Sync arrives."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "copy_abort_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    created = []

    def query(sock, sql, rows=None, state=None, ready=b"I"):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    try:
        second = socket.create_connection(
            (host if reference else "127.0.0.1", server["port"]), timeout=120)
        if reference:
            client.startup_reference(second, user, database, password)
        else:
            client.startup(second, "alice", "info")
        for extended in (False, True):
            for savepoint in (False, True):
                prefix = "copy_abort_" + uuid.uuid4().hex
                target, prior = prefix + "_target", prefix + "_prior"
                key = int(uuid.uuid4().hex[:8], 16)
                for table in (target, prior):
                    query(first, f"CREATE TABLE {table}(id INT PRIMARY KEY,v INT NOT NULL);")
                    created.append(table)
                query(first, f"INSERT INTO {target} VALUES(1,1),(2,2);")
                query(first, "BEGIN;", ready=b"T")
                query(first, "SET lock_timeout=50;", ready=b"T")
                query(second, "BEGIN;", ready=b"T")
                query(second, f"INSERT INTO {prior} VALUES(1,1);", ready=b"T")
                query(second, f"SELECT pg_advisory_xact_lock({key});", ready=b"T")
                if savepoint:
                    query(second, "SAVEPOINT __dbms_wire_copy_user;", ready=b"T")
                query(second, f"INSERT INTO {prior} VALUES(2,2);", ready=b"T")
                query(second, f"SELECT id FROM {target} WHERE id=2 FOR UPDATE;", [["2"]], ready=b"T")
                query(second, f"SELECT pg_advisory_xact_lock({key + 1});", ready=b"T")
                query(second, f"SELECT pg_advisory_lock({key + 2});", ready=b"T")
                sql = f"COPY {target}(id,v) FROM STDIN;".encode()
                if extended:
                    parse = b"\0" + sql + b"\0" + struct.pack("!H", 0)
                    bind = b"\0\0" + struct.pack("!HHH", 0, 0, 0)
                    execute = b"\0" + struct.pack("!I", 0)
                    second.sendall(client.typed(b"P", parse) +
                                   client.typed(b"B", bind) +
                                   client.typed(b"E", execute) + client.typed(b"H", b""))
                    assert client.read_message(second) == (b"1", b"")
                    assert client.read_message(second) == (b"2", b"")
                else:
                    second.sendall(client.typed(b"Q", sql + b"\0"))
                kind, body = client.read_message(second)
                assert (kind, body) == (b"G", b"\0" + struct.pack("!H", 2) + b"\0\0" * 2)
                second.sendall(client.typed(b"d", b"3\t3\n4\t\\N\n"))
                error = client.read_message(second)
                assert error[0] == b"E", error
                assert runner.decode_wire_result([error])[1] == "23502", error
                # The failed COPY backend is still waiting for client input.
                # Neither CopyDone nor Sync has been sent yet.
                query(first, f"SELECT id FROM {target} WHERE id=2 FOR UPDATE;", [["2"]], ready=b"T")
                query(first, f"SELECT pg_try_advisory_xact_lock({key + 1});", [["t"]], ready=b"T")
                query(first, f"SELECT pg_try_advisory_xact_lock({key});", [["f" if savepoint else "t"]], ready=b"T")
                query(first, f"SELECT pg_try_advisory_lock({key + 2});", [["f"]], ready=b"T")
                second.sendall(client.typed(b"c", b"") +
                               (client.typed(b"S", b"") if extended else b""))
                assert client.read_until_ready(second)[-1] == (b"Z", b"E")
                query(second, f"COPY {target}(id,v) FROM STDIN;", [], "25P02", b"E")
                query(second, "SELECT 1;", [], "25P02", b"E")
                if savepoint:
                    query(second, "ROLLBACK TO SAVEPOINT __dbms_wire_copy_user;", ready=b"T")
                    query(second, f"SELECT id,v FROM {prior} ORDER BY id;", [["1", "1"]], ready=b"T")
                    query(second, "COMMIT;")
                else:
                    query(second, "ROLLBACK;")
                query(second, f"SELECT pg_advisory_unlock({key + 2});", [["t"]])
                query(first, "ROLLBACK;")
                query(first, f"SELECT id,v FROM {target} ORDER BY id;", [["1", "1"], ["2", "2"]])
                query(first, f"SELECT id,v FROM {prior} ORDER BY id;", [["1", "1"]] if savepoint else [])
                for table in (prior, target):
                    query(first, f"DROP TABLE {table};")
                    created.remove(table)
        print("[COPY ERROR LOCK RELEASE " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if second is not None:
            second.close()
        if reference:
            try:
                client.simple_query(first, "ROLLBACK;")
                for table in reversed(created):
                    client.simple_query(first, f"DROP TABLE {table};")
            finally:
                first.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
