"""Month/day interval arithmetic over literals, rows, text Bind and DML."""
import calendar
import datetime
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid

DATES = ((2023, 1, 30), (2024, 1, 30), (2024, 3, 31),
         (1900, 1, 30), (2000, 1, 30), (2100, 1, 30),
         (2024, 12, 30), (2024, 8, 31), (2024, 2, 29))
STEPS = ((1, 1), (1, -1), (-1, 1), (-1, -1), (13, 31))
KINDS = (("date", 1082), ("timestamp", 1114), ("timestamptz", 1184))
EPOCH = datetime.datetime(2000, 1, 1)


def shifted(source, months, days, micros=0):
    index = source.year * 12 + source.month - 1 + months
    year, month = divmod(index, 12)
    month += 1
    # Month adjustment/clamp and day displacement are intentionally separate.
    point = source.replace(year=year, month=month,
                           day=min(source.day, calendar.monthrange(year, month)[1]))
    return point + datetime.timedelta(days=days, microseconds=micros)


def text(point, oid):
    if point is None:
        return None
    value = point.strftime("%Y-%m-%d %H:%M:%S")
    if point.microsecond:
        value += "." + ("%06d" % point.microsecond).rstrip("0")
    return value + ("+00" if oid == 1184 else "")


def datum(point, oid, binary):
    if point is None:
        return None
    if not binary:
        return text(point, oid).encode()
    delta = point - EPOCH
    micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
    return struct.pack("!q", micros)


def source_point(date, kind):
    return datetime.datetime(*date, **({} if kind == "date"
        else dict(hour=12, minute=34, second=56, microsecond=1)))


def input_text(point, kind):
    if kind == "date":
        return point.strftime("%Y-%m-%d")
    return text(point, 1184 if kind == "timestamptz" else 1114)


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("calendar_interval_runner",
        root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
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
    tx_state = b"I"

    def messages(mode, sql, params=None):
        if mode == "simple" and params is None:
            return client.simple_query(sock, sql)
        params = [] if params is None else params
        oids = [oid for oid, _ in params]
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(oids))
        parse += b"".join(struct.pack("!I", oid) for oid in oids)
        bind = b"\0\0" + struct.pack("!HH", 0, len(params))
        for _, value in params:
            bind += struct.pack("!i", -1 if value is None else len(value))
            if value is not None:
                bind += value
        bind += struct.pack("!HH", 1, int(mode == "binary"))
        packet = client.typed(b"P", parse) + client.typed(b"B", bind)
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def check(mode, sql, expected_rows=None, oid=None, tag=None, state=None,
              ready=None, params=None):
        nonlocal checks
        result = messages(mode, sql, params)
        errors = [client.diagnostic_fields(body).get(b"C") for k, body in result if k == b"E"]
        fields = client.row_description_fields(result) if any(k == b"T" for k, _ in result) else []
        actual_fields = [(c[0].decode(), c[3], c[4], c[5], c[6]) for c in fields]
        wanted_fields = [] if oid is None else [("shifted", oid, 8, -1, int(mode == "binary"))]
        rows = client.data_row_values(result)
        wanted_rows = [] if expected_rows is None else [[datum(v, oid, mode == "binary")] for v in expected_rows]
        tags = [body[:-1].decode() for k, body in result if k == b"C"]
        good = (errors == ([] if state is None else [state.encode()]) and
                actual_fields == wanted_fields and rows == wanted_rows and
                tags == ([] if state else [tag]) and
                result[-1] == (b"Z", tx_state if ready is None else ready))
        if not state and (mode != "simple" or params is not None):
            good = good and sum(k == b"1" for k, _ in result) == 1 and sum(k == b"2" for k, _ in result) == 1
        checks += 1
        detail = (actual_fields, rows, errors, tags, result[-1])
        print("CALENDAR_PROTOCOL", checks, mode, repr(sql), "match", good, repr(detail), flush=True)
        if not good:
            failures.append((checks, mode, sql, detail))

    def command(sql, tag, state=None, ready=None):
        return check("simple", sql, tag=tag, state=state, ready=ready)

    try:
        command("SET TIME ZONE 'UTC'", "SET")
        for mode_no, mode in enumerate(("simple", "text", "binary")):
            for date in DATES:
                for months, days in STEPS:
                    iv = "%d months %d days" % (months, days)
                    for kind, _ in KINDS:
                        source = source_point(date, kind)
                        expression = "%s '%s'" % (kind, input_text(source, kind))
                        oid = 1184 if kind == "timestamptz" else 1114
                        for op, sign in (("+", 1), ("-", -1)):
                            sql = "SELECT %s %s INTERVAL '%s' AS shifted" % (expression, op, iv)
                            check(mode, sql, [shifted(source, sign * months, sign * days)],
                                  oid, "SELECT 1")
            for expression, value in (
                ("TIMESTAMP '2024-01-30 23:59:59.999999'+INTERVAL '1 month 1 day 1 microsecond'", datetime.datetime(2024, 3, 2)),
                ("TIMESTAMP '2024-03-31'-INTERVAL '1 month 1 day 1 microsecond'", datetime.datetime(2024, 2, 27, 23, 59, 59, 999999)),
                ("TIMESTAMP '2024-01-30'+INTERVAL '1 month 1 day -1 microsecond'", datetime.datetime(2024, 2, 29, 23, 59, 59, 999999)),
                ("TIMESTAMP '2023-01-30'+INTERVAL '1 month 1 day 24 hours'", datetime.datetime(2023, 3, 2)),
                ("TIMESTAMP '2024-01-30 24:00:00'+INTERVAL '1 month -1 day'", datetime.datetime(2024, 2, 28)),
                ("TIMESTAMP '2024-01-30'+INTERVAL '1 month 1 day'-INTERVAL '1 month 1 day'", datetime.datetime(2024, 1, 31)),
                ("TIMESTAMP '2024-01-30'+(INTERVAL '1 month'+INTERVAL '1 day')", datetime.datetime(2024, 3, 1)),
                ("TIMESTAMP '2024-01-30'-(INTERVAL '-1 month'-INTERVAL '1 day')", datetime.datetime(2024, 3, 1)),
            ):
                check(mode, "SELECT " + expression + " AS shifted", [value], 1114, "SELECT 1")
            # Explicit CAST has its own SQL contract. Raw commuted wire type
            # inference is a separate known gap; native tests check its value.
            for kind, _ in KINDS:
                source = source_point((2024, 1, 30), kind)
                target = "timestamptz" if kind == "timestamptz" else "timestamp"
                oid = 1184 if kind == "timestamptz" else 1114
                sql = "SELECT CAST((INTERVAL '1 month 1 day'+%s '%s') AS %s) AS shifted" % (
                    kind, input_text(source, kind), target)
                check(mode, sql, [shifted(source, 1, 1)], oid, "SELECT 1")
            for expression, oid in (
                ("NULL::timestamp+INTERVAL '1 month 1 day'", 1114),
                ("TIMESTAMP '2024-01-30'+NULL::interval", 1114),
                ("NULL::timestamptz-INTERVAL '1 month 1 day'", 1184),
                ("CAST((NULL::date+INTERVAL '1 month 1 day') AS timestamp)", 1114),
            ):
                check(mode, "SELECT " + expression + " AS shifted", [None], oid, "SELECT 1")
            for kind, input_oid in KINDS:
                source = source_point((2024, 1, 30), kind)
                oid = 1184 if kind == "timestamptz" else 1114
                for op, sign in (("+", 1), ("-", -1)):
                    sql = "SELECT $1::%s %s $2::interval AS shifted" % (kind, op)
                    params = [(input_oid, input_text(source, kind).encode()),
                              (1186, b"1 month 1 day")]
                    check(mode, sql, [shifted(source, sign, sign)], oid, "SELECT 1", params=params)

            table = "calendar_" + uuid.uuid4().hex[:8] + "_" + str(mode_no)
            output = table + "_out"
            command("BEGIN", "BEGIN", ready=b"T")
            tx_state = b"T"
            command("CREATE TEMP TABLE " + table + "(id int,d date,ts timestamp,tz timestamptz,step interval)", "CREATE TABLE")
            command("INSERT INTO " + table + " VALUES"
                    "(1,'2024-01-30','2024-01-30 12:34:56.000001','2024-01-30 12:34:56.000001+00','1 month 1 day'),"
                    "(2,'2024-03-31','2024-03-31 12:34:56.000001','2024-03-31 12:34:56.000001+00','1 month 1 day'),"
                    "(3,NULL,NULL,NULL,'1 month 1 day')", "INSERT 0 3")
            for column, oid, kind in (("d", 1114, "date"), ("ts", 1114, "timestamp"), ("tz", 1184, "timestamptz")):
                points = [source_point(date, kind) for date in ((2024, 1, 30), (2024, 3, 31))]
                check(mode, "SELECT " + column + "+step AS shifted FROM " + table + " ORDER BY id",
                      [shifted(p, 1, 1) for p in points] + [None], oid, "SELECT 3")
            first = shifted(source_point((2024, 1, 30), "timestamp"), 1, 1)
            check(mode, "UPDATE " + table + " SET ts=ts+step WHERE id=1 RETURNING ts AS shifted",
                  [first], 1114, "UPDATE 1")
            command("SAVEPOINT calendar_keep", "SAVEPOINT")
            check(mode, "UPDATE " + table + " SET ts=ts-step WHERE id=1 RETURNING ts AS shifted",
                  [shifted(first, -1, -1)], 1114, "UPDATE 1")
            command("ROLLBACK TO calendar_keep", "ROLLBACK")
            check(mode, "SELECT ts AS shifted FROM " + table + " WHERE id=1", [first], 1114, "SELECT 1")
            command("CREATE TEMP TABLE " + output + "(id int,ts timestamp)", "CREATE TABLE")
            command("INSERT INTO " + output + " SELECT id,ts+step FROM " + table + " WHERE id<3", "INSERT 0 2")
            check(mode, "SELECT ts AS shifted FROM " + output + " ORDER BY id",
                  [shifted(first, 1, 1), shifted(source_point((2024, 3, 31), "timestamp"), 1, 1)],
                  1114, "SELECT 2")
            command("SAVEPOINT calendar_error", "SAVEPOINT")
            command("SELECT INTERVAL 'not_an_interval'", None, state="22007", ready=b"E")
            command("SELECT 7", None, state="25P02", ready=b"E")
            command("ROLLBACK TO calendar_error", "ROLLBACK")
            check(mode, "SELECT ts+step AS shifted FROM " + table + " WHERE id=1",
                  [shifted(first, 1, 1)], 1114, "SELECT 1")
            command("COMMIT", "COMMIT", ready=b"I")
            tx_state = b"I"
            check(mode, "SELECT ts AS shifted FROM " + table + " WHERE id=1", [first], 1114, "SELECT 1")
            command("DROP TABLE " + output, "DROP TABLE")
            command("DROP TABLE " + table, "DROP TABLE")
        expected = 943
        print("CALENDAR_PROTOCOL_COMPLETE", checks, "EXPECTED", expected,
              "FAILURES", len(failures), flush=True)
        assert checks == expected and not failures, (checks, expected, len(failures), failures[:16])
    finally:
        try:
            if tx_state != b"I":
                client.simple_query(sock, "ROLLBACK")
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == "__main__":
    main()
