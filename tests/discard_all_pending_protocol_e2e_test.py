#!/usr/bin/env python3
"""DISCARD ALL distinguishes planning from executing an implicit block."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "discard_pending_runner", root / "tests/compat/pg_diff_runner.py")
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

    def pending(executed):
        messages = client.typed(b"P", b"prior\0SELECT 1\0\0\0")
        messages += client.typed(b"B", b"prior_portal\0prior\0" + struct.pack("!HHH", 0, 0, 0))
        if executed:
            messages += client.typed(b"E", b"prior_portal\0" + struct.pack("!I", 0))
        sock.sendall(messages + client.typed(b"H"))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")
        if executed:
            assert client.read_message(sock)[0] == b"D"
            assert client.read_message(sock) == (b"C", b"SELECT 1\0")

    def discard(extended):
        if extended:
            sock.sendall(client.typed(b"P", b"cleanup\0DISCARD ALL\0\0\0") +
                         client.typed(b"B", b"cleanup_portal\0cleanup\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"E", b"cleanup_portal\0" + struct.pack("!I", 0)) +
                         client.typed(b"S"))
            return client.read_until_ready(sock)
        return client.simple_query(sock, "DISCARD ALL;")

    try:
        def simple(sql, state=None, rows=None):
            messages = client.simple_query(sock, sql)
            result = runner.decode_wire_result(messages)
            assert result[1] == state and messages[-1] == (b"Z", b"I"), (sql, result, messages[-1])
            if rows is not None:
                assert result[0] == rows, (sql, result)

        simple("CREATE TEMP TABLE discard_pending_temp(id SERIAL);")
        simple("INSERT INTO discard_pending_temp DEFAULT VALUES RETURNING id;", rows=[["1"]])
        pending(False)
        messages = discard(True)
        assert runner.decode_wire_result(messages)[1] is None, messages
        assert messages[-1] == (b"Z", b"I"), messages
        simple("SELECT id FROM discard_pending_temp;", "42P01")
        simple("SELECT nextval('discard_pending_temp_id_seq');", "42P01")
        simple("DROP INDEX pg_temp.discard_pending_missing;", "42704")
        for extended in (False, True):
            for executed in (False, True):
                pending(executed)
                messages = discard(extended)
                rejected = executed
                result = runner.decode_wire_result(messages)
                assert result[1] == ("25001" if rejected else None), (extended, executed, result)
                assert messages[-1] == (b"Z", b"I"), messages
                if not rejected:
                    assert result[4] == "DISCARD ALL", result
                sock.sendall(client.typed(b"D", b"Sprior\0") + client.typed(b"S"))
                messages = client.read_until_ready(sock)
                result = runner.decode_wire_result(messages, include_types=True)
                assert result[1] == (None if rejected else "26000"), result
                if rejected:
                    assert (result[3], result[5]) == (["?column?"], [23]), result
                assert messages[-1] == (b"Z", b"I"), messages
                sock.sendall(client.typed(b"D", b"Pprior_portal\0") + client.typed(b"S"))
                messages = client.read_until_ready(sock)
                assert runner.decode_wire_result(messages)[1] == "34000", messages
                assert messages[-1] == (b"Z", b"I"), messages
                sock.sendall(client.typed(b"C", b"Sprior\0") +
                             client.typed(b"C", b"Scleanup\0") + client.typed(b"S"))
                assert client.read_until_ready(sock) == [(b"3", b""), (b"3", b""), (b"Z", b"I")]
        for planning in (False, True):
            if planning:
                pending(False)
            sock.sendall(client.typed(b"P", b"cleanup\0DISCARD ALL\0\0\0") +
                         client.typed(b"B", b"cleanup_portal\0cleanup\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"E", b"cleanup_portal\0" + struct.pack("!I", 0)) +
                         client.typed(b"H"))
            assert client.read_message(sock) == (b"1", b"")
            assert client.read_message(sock) == (b"2", b"")
            assert client.read_message(sock) == (b"C", b"DISCARD ALL\0")
            # The immediate commit expires even the executing utility portal
            # before Sync, not only after ReadyForQuery has been sent.
            sock.sendall(client.typed(b"D", b"Pcleanup_portal\0") + client.typed(b"H"))
            message = client.read_message(sock)
            assert runner.decode_wire_result([message])[1] == "34000", message
            sock.sendall(client.typed(b"S"))
            assert client.read_until_ready(sock) == [(b"Z", b"I")]
        assert client.simple_query(sock, "BEGIN;")[-1] == (b"Z", b"T")
        pending(False)
        messages = discard(True)
        assert runner.decode_wire_result(messages)[1] == "25001", messages
        assert messages[-1] == (b"Z", b"E"), messages
        assert client.simple_query(sock, "ROLLBACK;")[-1] == (b"Z", b"I")
        messages = client.simple_query(sock, "SELECT 11;")
        assert runner.decode_wire_result(messages)[0] == [["11"]], messages
        assert messages[-1] == (b"Z", b"I"), messages
        print("[DISCARD ALL PENDING " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
