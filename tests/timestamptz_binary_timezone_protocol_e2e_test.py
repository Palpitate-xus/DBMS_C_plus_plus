"""Binary TIMESTAMPTZ represents UTC instants, independent of session TimeZone.

Receiver timezone scope only: whole seconds, infinities and NULL; no claim
for fractional timestamp receivers, heap upgrades or malformed wire frames.
"""
import datetime
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "timestamptz_binary_timezone_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checks, failures = 0, []
    statement = b"r131_utc_instant\0"

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("TIMESTAMPTZ_BINARY_TIMEZONE", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def exchange(packet):
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def bind(raw, oid, input_format=1, expected=None, named=False):
        if input_format == 1:
            expected = raw
        value = struct.pack("!i", -1 if raw is None else len(raw)) + (raw or b"")
        packet = b"" if named else client.typed(
            b"P", b"\0SELECT $1 AS v\0" + struct.pack("!HI", 1, oid))
        packet += client.typed(b"B", b"\0" + (statement if named else b"\0") +
                               struct.pack("!HHH", 1, input_format, 1) + value + struct.pack("!HH", 1, 1))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        messages = exchange(packet)
        kinds = [kind for kind, _ in messages]
        fields = client.row_description_fields(messages) if b"T" in kinds else []
        rows = client.data_row_values(messages)
        tags = [body for kind, body in messages if kind == b"C"]
        good = messages[-1] == (b"Z", b"I") and not any(kind in (b"E", b"N") for kind in kinds)
        good = good and kinds.count(b"1") == (0 if named else 1) and kinds.count(b"2") == 1
        good = good and fields == [(b"v", 0, 0, oid, 8, -1, 1)]
        good = good and rows == [[expected]] and tags == [b"SELECT 1\0"]
        report(good, zone + " oid=" + str(oid) + " format=" + str(input_format) +
               (" named" if named else "") + " input=" + str(None if raw is None else raw.hex()),
               (fields, rows, tags, [client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]))

    epoch = datetime.datetime(2000, 1, 1)
    # Independent civil datetime differences, with exact whole-second math.
    dated = [datetime.datetime(2000, 2, 29), datetime.datetime(2024, 3, 10, 6, 59, 59),
             datetime.datetime(2024, 11, 3, 6)]
    micros = [0, -1000000, 1000000, -86400000000, 86400000000]
    for value in dated:
        delta = value - epoch
        micros.append((delta.days * 86400 + delta.seconds) * 1000000)
    micros += [(1 << 63) - 1, -(1 << 63), None]
    assert len(micros) == 11
    try:
        messages = exchange(client.typed(b"P", statement + b"SELECT $1 AS v\0" + struct.pack("!HI", 1, 1184)))
        kinds = [kind for kind, _ in messages]
        report(messages[-1] == (b"Z", b"I") and kinds.count(b"1") == 1 and
               not any(kind in (b"E", b"N", b"T", b"D", b"C") for kind in kinds), "named Parse", messages)
        messages = exchange(client.typed(b"D", b"S" + statement))
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        report(messages[-1] == (b"Z", b"I") and fields == [(b"v", 0, 0, 1184, 8, -1, 0)] and
               [body for kind, body in messages if kind == b"t"] == [struct.pack("!HI", 1, 1184)] and
               not any(kind in (b"E", b"N", b"D", b"C") for kind, _ in messages), "named Describe", messages)
        for zone in ("UTC", "Asia/Shanghai", "America/New_York", "Asia/Kathmandu",
                     "Pacific/Kiritimati", "Europe/Berlin"):
            messages = client.simple_query(sock, "SET TIME ZONE '" + zone + "'")
            report(messages[-1] == (b"Z", b"I") and
                   [body for kind, body in messages if kind == b"C"] == [b"SET\0"] and
                   not any(kind == b"E" for kind, _ in messages), "SET " + zone, messages)
            for oid in (1114, 1184):
                for value in micros:
                    bind(None if value is None else struct.pack("!q", value), oid)
            bind(b"2000-01-01 00:00:00", 1114, input_format=0, expected=struct.pack("!q", 0))
            bind(b"2000-01-01 00:00:00+00", 1184, input_format=0, expected=struct.pack("!q", 0))
            bind(struct.pack("!q", 0), 1184, named=True)
        print("TIMESTAMPTZ_BINARY_TIMEZONE_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 158 and not failures, (checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
