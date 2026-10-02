#!/usr/bin/env python3
"""SET TRANSACTION uses strict ordered mode grammar, not substring matching."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "set_transaction_modes_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "set_transaction_modes_" + uuid.uuid4().hex
    created = False

    def query(sql, extended=False, state=None, ready=b"T", tag=None, warning=False):
        if extended:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S", b""))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if tag is not None:
            assert result[4] == tag, (sql, result)
        notices = [client.diagnostic_fields(payload) for kind, payload in messages if kind == b"N"]
        if warning:
            assert len(notices) == 1 and notices[0].get(b"C") == b"25P01", (sql, notices)
            assert notices[0].get(b"S") == b"WARNING", (sql, notices)
        else:
            assert not notices, (sql, notices)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        invalid = ("", "WORK", "READ COMMITTED", "ISOLATION SERIALIZABLE", "ISOLATION LEVEL",
                   "READ ONLY,", ", READ ONLY", "READ ONLY,, READ WRITE", "READ ONLY GARBAGE")
        valid = (("READ ONLY, ISOLATION LEVEL SERIALIZABLE", True),
                 ("ISOLATION LEVEL SERIALIZABLE READ ONLY", True),
                 ("READ WRITE, READ ONLY", True), ("READ ONLY, READ WRITE", False),
                 ("NOT DEFERRABLE, ISOLATION LEVEL REPEATABLE READ, READ ONLY", True),
                 ("READ ONLY /* separator */ READ WRITE", False),
                 ("ISOLATION LEVEL READ COMMITTED, ISOLATION LEVEL SERIALIZABLE", False))
        for extended in (False, True):
            for modes in invalid:
                query("BEGIN;", extended)
                query("SET TRANSACTION " + modes + ";", extended, "42601", b"E")
                query("SET TRANSACTION READ WRITE;", extended, "25P02", b"E")
                query("ROLLBACK;", extended, ready=b"I")
                query("SET TRANSACTION " + modes + ";", extended, "42601", b"I")
            for modes, read_only in valid:
                query("BEGIN;", extended)
                query("SET TRANSACTION " + modes + ";", extended, tag="SET")
                query(f"INSERT INTO {table} VALUES(1);", extended,
                      "25006" if read_only else None, b"E" if read_only else b"T")
                query("ROLLBACK;", extended, ready=b"I")
            query("BEGIN;", extended)
            query("SELECT 1;", extended)
            query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;", extended, tag="SET")
            query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, ISOLATION LEVEL READ COMMITTED;",
                  extended, "25001", b"E")
            query("ROLLBACK;", extended, ready=b"I")
            query("BEGIN;", extended)
            query("SAVEPOINT child;", extended)
            query("SET TRANSACTION NOT DEFERRABLE;", extended, "25001", b"E")
            query("ROLLBACK TO child;", extended)
            query("RELEASE child;", extended)
            query("SET TRANSACTION NOT DEFERRABLE;", extended, tag="SET")
            query("ROLLBACK;", extended, ready=b"I")
            query("SET TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE;",
                  extended, ready=b"I", tag="SET", warning=True)
            query("BEGIN;", extended)
            query(f"INSERT INTO {table} VALUES(1);", extended)
            query("ROLLBACK;", extended, ready=b"I")
        print("[SET TRANSACTION MODES " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        try:
            client.simple_query(sock, "ROLLBACK;")
            if created:
                client.simple_query(sock, f"DROP TABLE {table};")
        finally:
            if reference:
                sock.close()
            else:
                runner.stop_ours(server)


if __name__ == "__main__":
    main()
