"""Scalar TIMESTAMPTZ result presentation, not fractional heap storage."""
import datetime
import importlib.util
from pathlib import Path
import socket
import struct
import sys
from zoneinfo import ZoneInfo


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("returning_timezone_runner", root / "tests/compat/pg_diff_runner.py")
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
    epoch = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    fraction = epoch + datetime.timedelta(microseconds=1)
    raw = b"2000-01-01 00:00:00"
    projection = ("id,v,n,CAST('2000-01-01 00:00:00.000001+00' AS timestamptz) AS computed,"
                  "NULL::timestamptz AS missing,'infinity'::timestamptz AS hi,"
                  "'-infinity'::timestamptz AS lo,'2000-01-01 00:00:00'::text AS raw")
    names = ("id", "v", "n", "computed", "missing", "hi", "lo", "raw")
    oids = (23, 1184, 1114, 1184, 1184, 1184, 1184, 25)
    sizes = (4, 8, 8, 8, 8, 8, 8, -1)

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("TIMESTAMPTZ_RETURNING_TIMEZONE", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def command(sql, tag):
        messages = client.simple_query(sock, sql)
        report(messages[-1] == (b"Z", b"I") and not any(kind == b"E" for kind, _ in messages) and
               [body for kind, body in messages if kind == b"C"] == [tag.encode() + b"\0"], sql, messages)

    def micros(point):
        delta = point - epoch
        return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds

    def display(point, zone):
        local = point.astimezone(ZoneInfo(zone))
        value = local.strftime("%Y-%m-%d %H:%M:%S")
        if local.microsecond:
            value += "." + ("%06d" % local.microsecond).rstrip("0")
        minutes = int(local.utcoffset().total_seconds()) // 60
        sign = "+" if minutes >= 0 else "-"
        hours, remainder = divmod(abs(minutes), 60)
        return (value + sign + "%02d" % hours + (":%02d" % remainder if remainder else "")).encode()

    def query(mode, sql, identifier, point, zone, tag):
        code = int(mode == "binary")
        if mode == "simple":
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
            packet += client.typed(b"B", b"\0\0" + struct.pack("!HHHH", 0, 0, 1, code))
            packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
            sock.sendall(packet + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        fields = client.row_description_fields(messages) if any(kind == b"T" for kind, _ in messages) else []
        metadata = [(field[0].decode(), field[3], field[4], field[5], field[6]) for field in fields]
        desired = [(name, oid, size, -1, code) for name, oid, size in zip(names, oids, sizes)]
        values = ([struct.pack("!i", identifier), struct.pack("!q", micros(point)), struct.pack("!q", 0),
                   struct.pack("!q", 1), None, struct.pack("!q", (1 << 63) - 1),
                   struct.pack("!q", -(1 << 63)), raw] if code else
                  [str(identifier).encode(), display(point, zone), raw, display(fraction, zone),
                   None, b"infinity", b"-infinity", raw])
        good = (messages[-1] == (b"Z", b"I") and not any(kind in (b"E", b"N") for kind, _ in messages) and
                metadata == desired and client.data_row_values(messages) == [values] and
                [body for kind, body in messages if kind == b"C"] == [tag.encode() + b"\0"])
        if mode != "simple":
            good = good and sum(kind == b"1" for kind, _ in messages) == 1 and sum(kind == b"2" for kind, _ in messages) == 1
        report(good, zone + " " + mode + " " + sql,
               (metadata, client.data_row_values(messages),
                [client.diagnostic_fields(body) for kind, body in messages if kind == b"E"]))

    try:
        command("CREATE " + ("TEMP " if reference else "") + "TABLE returning_timezone(id integer,v timestamptz,n timestamp)", "CREATE TABLE")
        identifier = 0
        for zone in ("UTC", "Asia/Shanghai", "America/New_York", "Asia/Kathmandu",
                     "Pacific/Kiritimati", "Europe/Berlin"):
            command("SET TIME ZONE '" + zone + "'", "SET")
            for mode in ("simple", "text", "binary"):
                guard = ("SELECT 1 AS id,'2000-01-01 00:00:00+00'::timestamptz AS v,"
                         "'2000-01-01 00:00:00'::timestamp AS n," + projection.split(",", 3)[3])
                query(mode, guard, 1, epoch, zone, "SELECT 1")
            for point in (epoch, datetime.datetime(2024, 7, 1, tzinfo=datetime.timezone.utc)):
                for mode in ("simple", "text", "binary"):
                    identifier += 1
                    instant = point.strftime("%Y-%m-%d %H:%M:%S") + "+00"
                    query(mode, "INSERT INTO returning_timezone VALUES(" + str(identifier) + ",'" + instant +
                          "','2000-01-01 00:00:00') RETURNING " + projection,
                          identifier, point, zone, "INSERT 0 1")
                    updated = point + datetime.timedelta(hours=1)
                    instant = updated.strftime("%Y-%m-%d %H:%M:%S") + "+00"
                    query(mode, "UPDATE returning_timezone SET v='" + instant + "' WHERE id=" + str(identifier) +
                          " RETURNING " + projection, identifier, updated, zone, "UPDATE 1")
                    query(mode, "DELETE FROM returning_timezone WHERE id=" + str(identifier) + " RETURNING " +
                          projection, identifier, updated, zone, "DELETE 1")
        # Capture each statement's display timezone before a later SET in
        # the same Simple Query batch; do not re-render all buffered results
        # under the final timezone at the time they are sent.
        batch = ("SET TIME ZONE 'UTC'; INSERT INTO returning_timezone VALUES(9999,"
                 "'2000-01-01 00:00:00+00','2000-01-01 00:00:00') RETURNING v; "
                 "SET TIME ZONE 'America/New_York'; DELETE FROM returning_timezone WHERE id=9999 RETURNING v")
        messages = client.simple_query(sock, batch)
        report(messages[-1] == (b"Z", b"I") and not any(kind in (b"E", b"N") for kind, _ in messages) and
               sum(kind == b"T" for kind, _ in messages) == 2 and
               client.data_row_values(messages) == [[display(epoch, "UTC")], [display(epoch, "America/New_York")]] and
               [body for kind, body in messages if kind == b"C"] ==
                   [b"SET\0", b"INSERT 0 1\0", b"SET\0", b"DELETE 1\0"],
               "multi-statement timezone captured per execution", messages)
        print("TIMESTAMPTZ_RETURNING_TIMEZONE_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 134 and not failures, (checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
