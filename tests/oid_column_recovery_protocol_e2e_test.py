#!/usr/bin/env python3
"""Check OID column and index data after crash recovery and a table rewrite."""

from pathlib import Path
import signal
import struct
import subprocess
import time

from session_setting_functions_protocol_e2e_test import Wire, runner


def main():
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    data = Path(server["dir"], "info")
    wire = Wire(client, server["sock"])

    def restart():
        nonlocal wire
        server["process"].kill()
        assert server["process"].wait(timeout=20) == -signal.SIGKILL
        server["sock"].close()
        server["sock"] = None
        diagnostic = data.parent / "oid-recovery.stderr"
        with diagnostic.open("wb") as log:
            server["process"] = subprocess.Popen(
                [runner.DBMS_MAIN, "--data-dir", server["dir"], "--server", str(server["port"]), "--insecure"],
                cwd=server["dir"], stdout=subprocess.DEVNULL, stderr=log)
        deadline = time.monotonic() + 20
        while True:
            try:
                runner.reconnect_ours(server, client)
                break
            except ConnectionRefusedError:
                assert server["process"].poll() is None and time.monotonic() < deadline, diagnostic.read_text()
                time.sleep(0.05)
        wire = Wire(client, server["sock"])

    def verify_key(key, ids, ready=b"I"):
        for predicate in (f"k='{key}'", f"k>='{key}' AND k<='{key}'"):
            wire.query("SELECT id FROM oid_durable WHERE " + predicate, rows=[(str(i),) for i in ids],
                       tag=f"SELECT {len(ids)}", fields=[("id", 23)], ready=ready)

    def verify_page(ids, ready=b"I"):
        ordered = "SELECT id FROM oid_durable ORDER BY k DESC NULLS LAST"
        wire.query(ordered + " LIMIT 2",
                   rows=[(str(i),) for i in ids], tag="SELECT 2", fields=[("id", 23)], ready=ready)
        frames = wire.exchange(wire.parse(ordered + " LIMIT $1") + wire.bind(arguments=(b"2",), result_format=1)
                               + wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        assert b"".join(kind for kind, _ in frames) == b"12TDDCZ", frames
        assert client.row_description_fields(frames)[0][3:] == (23, 4, -1, 1), frames
        assert client.data_row_values(frames) == [[struct.pack("!i", i)] for i in ids], frames
        assert frames[-2:] == [(b"C", b"SELECT 2\0"), (b"Z", ready)], frames

    def begin_update():
        wire.command("BEGIN", "BEGIN", ready=b"T")
        wire.command("UPDATE oid_durable SET k='0x10' WHERE id=2", "UPDATE 1", ready=b"T")
        verify_key("16", [2], ready=b"T")
        verify_key("0xffffffff", [], ready=b"T")
        verify_page([3, 2], ready=b"T")

    try:
        wire.command("CREATE TABLE oid_durable(id integer PRIMARY KEY,k oid,note text)", "CREATE TABLE")
        wire.command("INSERT INTO oid_durable VALUES (1,0,'zero'),(2,4294967295,'max'),"
                     "(3,2147483648,'high'),(4,NULL,'missing')", "INSERT 0 4")
        wire.command("CREATE INDEX oid_durable_key ON oid_durable(k)", "CREATE INDEX")
        assert struct.unpack_from("=I", (data / "oid_durable.stc").read_bytes())[0] == 0x44420010

        begin_update()
        restart()
        verify_key("0xffffffff", [2])
        verify_key("16", [])
        verify_page([2, 3])

        begin_update()
        wire.command("COMMIT", "COMMIT")
        restart()
        verify_key("4294967295", [])
        verify_key("16", [2])
        verify_page([3, 2])
        wire.command("VACUUM FULL oid_durable", "VACUUM")
        wire.command("REINDEX TABLE oid_durable", "REINDEX")
        restart()
        verify_key("16", [2])
        verify_page([3, 2])

        name = b"objects"
        request = (wire.parse("SELECT k FROM oid_durable ORDER BY id", name) + wire.bind(name, result_format=1)
                   + wire.describe_portal() + wire.execute() + wire.packet(b"S"))
        frames = wire.exchange(request)
        assert b"".join(kind for kind, _ in frames) == b"12TDDDDCZ", frames
        assert client.row_description_fields(frames)[0][3:] == (26, 4, -1, 1), frames
        expected = [[struct.pack("!I", n)] for n in (0, 16, 2147483648)] + [[None]]
        assert client.data_row_values(frames) == expected, frames
        print("OID_COLUMN_RECOVERY_PROTOCOL_COMPLETE")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
