"""Temporal arithmetic result ownership: no sampled values or operand OIDs."""
import datetime as dt
import importlib.util
from pathlib import Path
import socket
import struct
import sys

TYPES = {"date": (1082, 4), "timestamp": (1114, 8),
         "timestamptz": (1184, 8), "integer": (23, 4), "interval": (1186, 16)}
EPOCH = dt.datetime(2000, 1, 1)
# Every interval here has zero months/days: these independently isolate type
# ownership and do not depend on the pending R147 mixed-calendar value repair.
CASES = [
    ("+", "date", "interval", "timestamp", dt.datetime(2000, 1, 15, 0, 30)),
    ("-", "date", "interval", "timestamp", dt.datetime(2000, 1, 14, 23, 30)),
    ("+", "timestamp", "interval", "timestamp", dt.datetime(2000, 1, 15, 13, 4, 56, 1)),
    ("-", "timestamp", "interval", "timestamp", dt.datetime(2000, 1, 15, 12, 4, 56, 1)),
    ("+", "timestamptz", "interval", "timestamptz", dt.datetime(2000, 1, 15, 13, 4, 56, 1)),
    ("-", "timestamptz", "interval", "timestamptz", dt.datetime(2000, 1, 15, 12, 4, 56, 1)),
    ("+", "interval", "date", "timestamp", dt.datetime(2000, 1, 14, 1)),
    ("+", "interval", "timestamp", "timestamp", dt.datetime(2000, 1, 14, 12, 34, 56, 1)),
    ("+", "interval", "timestamptz", "timestamptz", dt.datetime(2000, 1, 14, 12, 34, 56, 1)),
    ("-", "date", "date", "integer", 1),
    ("-", "timestamp", "timestamp", "interval", (1, 3600000000)),
    ("-", "timestamptz", "timestamptz", "interval", (1, 3600000000)),
    ("-", "timestamp", "timestamptz", "interval", (1, 3600000000)),
    ("-", "timestamptz", "timestamp", "interval", (1, 3600000000)),
    ("+", "date", "integer", "date", dt.date(2000, 1, 18)),
    ("-", "date", "integer", "date", dt.date(2000, 1, 12)),
    ("+", "integer", "date", "date", dt.date(2000, 1, 17)),
    ("+", "interval", "interval", "interval", (0, 5400000000)),
    ("-", "interval", "interval", "interval", (0, 1800000000)),
]


def input_text(kind, left):
    if kind == "integer":
        return b"3"
    if kind == "date":
        return b"2000-01-15" if left else b"2000-01-14"
    if kind == "interval":
        return b"1 hour" if left else b"30 minutes"
    value = "2000-01-15 12:34:56.000001" if left else "2000-01-14 11:34:56.000001"
    return (value + ("+00" if kind == "timestamptz" else "")).encode()


def text(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return str(value).encode()
    if kind == "date":
        return value.isoformat().encode()
    if kind == "interval":
        days, micros = value
        hours, rest = divmod(micros, 3600000000)
        minutes, rest = divmod(rest, 60000000)
        seconds, fraction = divmod(rest, 1000000)
        assert not fraction
        return (("%d day " % days if days else "") +
                "%02d:%02d:%02d" % (hours, minutes, seconds)).encode()
    output = value.strftime("%Y-%m-%d %H:%M:%S")
    if value.microsecond:
        output += "." + ("%06d" % value.microsecond).rstrip("0")
    return (output + ("+00" if kind == "timestamptz" else "")).encode()


def datum(value, kind, binary):
    if value is None or not binary:
        return text(value, kind)
    if kind == "integer":
        return struct.pack("!i", value)
    if kind == "date":
        return struct.pack("!i", (value - EPOCH.date()).days)
    if kind == "interval":
        days, micros = value
        return struct.pack("!qii", micros, days, 0)
    delta = value - EPOCH
    return struct.pack("!q", (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds)


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("temporal_types_runner",
        root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert reference or not sys.argv[1:]
    server = None
    if reference:
        h, p, u, d, pw = runner._reference_connection_settings()
        sock = socket.create_connection((h, p), timeout=runner.wire_timeout())
        client.startup_reference(sock, u, d, pw)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]
    checks, failures = 0, []

    def parse(sql, parameters):
        oids = [] if parameters is None else [TYPES[k][0] for k, _ in parameters]
        return b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(oids)) + b"".join(
            struct.pack("!I", oid) for oid in oids)

    def bind(parameters, binary):
        parameters = [] if parameters is None else parameters
        payload = b"\0\0" + struct.pack("!HH", 0, len(parameters))
        for _, value in parameters:
            payload += struct.pack("!i", -1 if value is None else len(value))
            if value is not None:
                payload += value
        return payload + struct.pack("!HH", 1, int(binary))

    def execute(mode, sql, parameters=None):
        if mode == "simple" and parameters is None:
            return client.simple_query(sock, sql)
        sock.sendall(client.typed(b"P", parse(sql, parameters)) +
                     client.typed(b"B", bind(parameters, mode == "binary")) +
                     client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                     client.typed(b"S"))
        return client.read_until_ready(sock)

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("TEMPORAL_TYPES_PROTOCOL", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((checks, label, detail))

    def fields(messages):
        return [(f[0].decode(), f[3], f[4], f[5], f[6])
                for f in client.row_description_fields(messages)] if any(k == b"T" for k, _ in messages) else []

    def query(mode, sql, kind, expected, empty=False, parameters=None):
        messages = execute(mode, sql, parameters)
        oid, length = TYPES[kind]
        errors = [client.diagnostic_fields(body) for k, body in messages if k == b"E"]
        rows = client.data_row_values(messages)
        wanted = [] if empty else [[datum(expected, kind, mode == "binary")]]
        tags = [body[:-1] for k, body in messages if k == b"C"]
        good = (not errors and fields(messages) == [("v", oid, length, -1, int(mode == "binary"))] and
                rows == wanted and tags == [b"SELECT 0" if empty else b"SELECT 1"] and
                messages[-1] == (b"Z", b"I"))
        if mode != "simple" or parameters is not None:
            good = good and sum(k == b"1" for k, _ in messages) == 1 and sum(k == b"2" for k, _ in messages) == 1
        report(good, (mode, sql, parameters), (fields(messages), rows, errors, tags, messages[-1]))

    try:
        messages = client.simple_query(sock, "SET TIME ZONE 'UTC'")
        report(not any(k == b"E" for k, _ in messages) and messages[-1] == (b"Z", b"I"),
               "UTC", messages)
        for mode in ("simple", "text", "binary"):
            for op, left, right, result, value in CASES:
                literals = ["CAST('%s' AS %s)" % (input_text(kind, side).decode(), kind)
                            for kind, side in ((left, True), (right, False))]
                for null_left, null_right in ((False, False), (True, False), (False, True), (True, True)):
                    lhs = "CAST(NULL AS %s)" % left if null_left else literals[0]
                    rhs = "CAST(NULL AS %s)" % right if null_right else literals[1]
                    sql = "SELECT %s %s %s AS v" % (lhs, op, rhs)
                    query(mode, sql, result, None if null_left or null_right else value)
                sql = "SELECT %s %s %s AS v WHERE FALSE" % (literals[0], op, literals[1])
                query(mode, sql, result, None, empty=True)
                sql = "SELECT CAST($1 AS %s) %s CAST($2 AS %s) AS v" % (left, op, right)
                for null_left, null_right in ((False, False), (True, False), (False, True), (True, True)):
                    parameters = [(left, None if null_left else input_text(left, True)),
                                  (right, None if null_right else input_text(right, False))]
                    query(mode, sql, result, None if null_left or null_right else value, parameters=parameters)
        for op, left, right, result, value in CASES:
            sql = "SELECT CAST($1 AS %s) %s CAST($2 AS %s) AS v" % (left, op, right)
            parameters = [(left, input_text(left, True)), (right, input_text(right, False))]
            oid, length = TYPES[result]
            sock.sendall(client.typed(b"P", parse(sql, parameters)) +
                         client.typed(b"D", b"S\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
            descriptions = [body for k, body in messages if k == b"t"]
            expected_parameters = struct.pack("!HII", 2, TYPES[left][0], TYPES[right][0])
            report(fields(messages) == [("v", oid, length, -1, 0)] and
                   descriptions == [expected_parameters] and
                   sum(k == b"1" for k, _ in messages) == 1 and
                   not any(k in (b"D", b"C", b"E") for k, _ in messages) and
                   messages[-1] == (b"Z", b"I"), ("statement only", sql),
                   (fields(messages), messages))
            sock.sendall(client.typed(b"B", bind(parameters, True)) +
                         client.typed(b"D", b"P\0") + client.typed(b"S"))
            messages = client.read_until_ready(sock)
            report(fields(messages) == [("v", oid, length, -1, 1)] and
                   sum(k == b"2" for k, _ in messages) == 1 and
                   not any(k in (b"D", b"C", b"E") for k, _ in messages) and
                   messages[-1] == (b"Z", b"I"), ("portal only", sql),
                   (fields(messages), messages))
        expected = 552
        print("TEMPORAL_TYPES_PROTOCOL_COMPLETE", checks, "EXPECTED", expected,
              "FAILURES", len(failures), flush=True)
        assert checks == expected and not failures, (checks, expected, len(failures), failures[:20])
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
