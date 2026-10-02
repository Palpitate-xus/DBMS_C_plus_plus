#!/usr/bin/env python3
"""Even NOT DEFERRABLE must obey first-snapshot and user-subtransaction guards."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "not_deferrable_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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

    def query(sql, extended, state=None, ready=b"T", rows=None, repeated=False):
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
        if rows is not None:
            assert result[0] == rows, (sql, result)
        if repeated:
            notices = [client.diagnostic_fields(payload) for kind, payload in messages if kind == b"N"]
            assert any(fields.get(b"C") == b"25001" and fields.get(b"S") == b"WARNING"
                       for fields in notices), (sql, notices)
            if state is not None:
                assert next(i for i, item in enumerate(messages) if item[0] == b"N") < next(
                    i for i, item in enumerate(messages) if item[0] == b"E"), (sql, messages)

    try:
        for extended in (False, True):
            for prefix in ("BEGIN", "START TRANSACTION"):
                for separator in (" ", ", "):
                    change = prefix + " NOT DEFERRABLE" + separator + "NOT DEFERRABLE;"
                    query("BEGIN NOT DEFERRABLE;", extended)
                    query(change, extended, repeated=True)
                    query("ROLLBACK;", extended, ready=b"I")
                    query("BEGIN;", extended)
                    query("SELECT 1;", extended, rows=[["1"]])
                    query(change, extended, "25001", b"E", repeated=True)
                    query("ROLLBACK;", extended, ready=b"I")
                    query("BEGIN;", extended)
                    query("SAVEPOINT child;", extended)
                    query(change, extended, "25001", b"E", repeated=True)
                    query("ROLLBACK TO child;", extended)
                    query("RELEASE child;", extended)
                    query(change, extended, repeated=True)
                    query("ROLLBACK;", extended, ready=b"I")
                for ending in ("RELEASE child;", "ROLLBACK TO child;"):
                    query("BEGIN;", extended)
                    query("SAVEPOINT child;", extended)
                    query("SELECT 1;", extended, rows=[["1"]])
                    query(ending, extended)
                    if ending.startswith("ROLLBACK"):
                        query("RELEASE child;", extended)
                    query(prefix + " NOT DEFERRABLE;", extended, "25001", b"E", repeated=True)
                    query("ROLLBACK;", extended, ready=b"I")
            query("BEGIN;", extended)
            query("BEGIN NOT DEFERRABLE;", extended, repeated=True)
            query("COMMIT;", extended, ready=b"I")
        print("[NOT DEFERRABLE CHARACTERISTIC " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
