"""Fractional scalar temporal heap data: text/binary, indexes and cold restart."""
import datetime
import importlib.util
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid
from zoneinfo import ZoneInfo


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("temporal_heap_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ["--reference18"]
    assert not sys.argv[1:] or reference
    server, schema = None, None
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
    transaction = False

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("TEMPORAL_HEAP_MICROSECONDS", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def messages(mode, sql):
        if mode == "simple":
            return client.simple_query(sock, sql)
        packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        packet += client.typed(b"B", b"\0\0" + struct.pack("!HHHH", 0, 0, 1, int(mode == "binary")))
        packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sock.sendall(packet + client.typed(b"S"))
        return client.read_until_ready(sock)

    def command(sql, tag=None, state=None):
        nonlocal transaction
        result = messages("simple", sql)
        if state:
            errors = [client.diagnostic_fields(body).get(b"C") for kind, body in result if kind == b"E"]
            good = errors == [state.encode()]
        else:
            good = not any(kind == b"E" for kind, _ in result)
            if tag is not None:
                good = good and [body.rstrip(b"\0").decode() for kind, body in result if kind == b"C"] == [tag]
        if sql == "BEGIN":
            transaction = True
        elif sql in ("COMMIT", "ROLLBACK"):
            transaction = False
        report(good and result[-1] == (b"Z", b"T" if transaction else b"I"), sql, result)

    def query(mode, sql, fields, rows, tag):
        result = messages(mode, sql)
        columns = client.row_description_fields(result) if any(kind == b"T" for kind, _ in result) else []
        actual = [(c[0].decode(), c[3], c[4], c[5], c[6]) for c in columns]
        desired = [(name, oid, length, modifier, int(mode == "binary"))
                   for name, oid, length, modifier in fields]
        errors = [client.diagnostic_fields(body) for kind, body in result if kind == b"E"]
        good = (not errors and actual == desired and client.data_row_values(result) == rows and
                [body.rstrip(b"\0").decode() for kind, body in result if kind == b"C"] == [tag] and
                result[-1] == (b"Z", b"T" if transaction else b"I"))
        if mode != "simple":
            good = good and sum(k == b"1" for k, _ in result) == 1 and sum(k == b"2" for k, _ in result) == 1
        report(good, mode + " " + sql, (actual, client.data_row_values(result), errors))

    def text(kind, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if kind == "time":
            if value == 86400000000:
                return "24:00:00"
            seconds, fraction = divmod(value, 1000000)
            hours, seconds = divmod(seconds, 3600)
            minutes, seconds = divmod(seconds, 60)
            result = "%02d:%02d:%02d" % (hours, minutes, seconds)
        else:
            point = epoch + datetime.timedelta(microseconds=value)
            result = point.strftime("%Y-%m-%d %H:%M:%S")
            fraction = point.microsecond
        if fraction:
            result += "." + ("%06d" % fraction).rstrip("0")
        return result + ("+00" if kind == "timestamptz" else "")

    def cell(kind, value, mode):
        if value is None:
            return None
        if mode == "binary":
            number = {"infinity": (1 << 63) - 1, "-infinity": -(1 << 63)}.get(value, value)
            return struct.pack("!q", number)
        return text(kind, value).encode()

    def literal(kind, value):
        return "NULL" if value is None else "'" + text(kind, value) + "'"

    tables = []
    modes = ("simple", "text", "binary")
    kinds = (("timestamp", 1114, [-1, 0, 1, 2, 999999, "-infinity", "infinity", None]),
             ("timestamptz", 1184, [-1, 0, 1, 2, 999999, "-infinity", "infinity", None]),
             ("time", 1083, [0, 1, 2, 999999, 1000000, 86399999999, 86400000000, None]))

    def fields(oid, mixed):
        return [("id", 23, 4, -1), ("v", oid, 8, -1)] + ([("note", 25, -1, -1)] if mixed else [])

    def result_rows(kind, data, mixed, mode):
        return [[struct.pack("!i", key) if mode == "binary" else str(key).encode(), cell(kind, value, mode)] +
                ([b'word "quoted" label'] if mixed else []) for key, value in data]

    def phase(table, kind, oid, data, mixed, label):
        projection = "id,v,note" if mixed else "id,v"
        for mode in modes:
            query(mode, "SELECT " + projection + " FROM " + table + " ORDER BY id",
                  fields(oid, mixed), result_rows(kind, sorted(data.items()), mixed, mode), "SELECT " + str(len(data)))
        for key in (2, 3):
            for mode in modes:
                query(mode, "SELECT " + projection + " FROM " + table + " WHERE v=" +
                      literal(kind, data[key]) + "::" + kind + " ORDER BY id",
                      fields(oid, mixed),
                      result_rows(kind, [(i, v) for i, v in sorted(data.items()) if v == data[key]], mixed, mode),
                      "SELECT " + str(sum(v == data[key] for v in data.values())))
        print("TEMPORAL_HEAP_PHASE", table, label, flush=True)

    def restart():
        nonlocal sock
        if reference:
            sock.close()
            sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
            client.startup_reference(sock, user, database, password)
            command("SET search_path TO " + schema, "SET")
            command("SET TIME ZONE 'UTC'", "SET")
            report(True, "fresh reference backend (not a PostgreSQL server restart)", [])
            return
        work = Path(server["dir"]).resolve()
        assert work.parent == Path(tempfile.gettempdir()).resolve() and work.name.startswith("dbms-pgdiff-")
        sock.close()
        server["sock"] = None
        server["process"].terminate()
        try:
            server["process"].wait(timeout=10)
        except subprocess.TimeoutExpired:
            server["process"].kill()
            server["process"].wait(timeout=10)
            raise AssertionError("server did not stop cleanly; cold durability not established")
        server["process"] = subprocess.Popen(
            [runner.DBMS_MAIN, "--data-dir", str(work), "--server", str(server["port"]), "--insecure"],
            cwd=work, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while True:
            try:
                sock = socket.create_connection(("127.0.0.1", server["port"]), timeout=runner.wire_timeout())
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        server["sock"] = sock
        client.startup(sock, "alice", "info")
        report(True, "actual owned local server cold restart, original data directory retained", [])

    try:
        if reference:
            schema = "temporal_micro_" + uuid.uuid4().hex[:16]
            command("CREATE SCHEMA " + schema, "CREATE SCHEMA")
            command("SET search_path TO " + schema, "SET")
        command("SET TIME ZONE 'UTC'", "SET")
        for kind, oid, values in kinds:
            for mixed in (False, True):
                table = "micro_" + kind + ("_mixed" if mixed else "_fixed")
                data = dict(enumerate(values, 1))
                projection = "id,v,note" if mixed else "id,v"
                command("CREATE TABLE " + table + "(id integer PRIMARY KEY,v " + kind +
                        (",note text" if mixed else "") + ")", "CREATE TABLE")
                for key, value in data.items():
                    mode = modes[(key - 1) % len(modes)]
                    query(mode, "INSERT INTO " + table + " VALUES(" + str(key) + "," + literal(kind, value) +
                          (",'word \"quoted\" label'" if mixed else "") + ") RETURNING " + projection,
                          fields(oid, mixed), result_rows(kind, [(key, value)], mixed, mode), "INSERT 0 1")
                phase(table, kind, oid, data, mixed, "warm scan")
                def order_key(item):
                    v = item[1]
                    return (v is None, -float("inf") if v == "-infinity" else float("inf") if v == "infinity" else v or 0)
                ordered = sorted(data.items(), key=order_key)
                for mode in modes:
                    query(mode, "SELECT " + projection + " FROM " + table + " ORDER BY v,id",
                          fields(oid, mixed), result_rows(kind, ordered, mixed, mode), "SELECT 8")
                data[3] = 123456
                query("text", "UPDATE " + table + " SET v=" + literal(kind, data[3]) + " WHERE id=3 RETURNING " +
                      projection, fields(oid, mixed), result_rows(kind, [(3, data[3])], mixed, "text"), "UPDATE 1")
                command("CREATE INDEX " + table + "_v_idx ON " + table + "(v)", "CREATE INDEX")
                phase(table, kind, oid, data, mixed, "indexed equality")
                command("REINDEX TABLE " + table, "REINDEX")
                command("ALTER TABLE " + table + " ALTER COLUMN v TYPE " + kind, "ALTER TABLE")
                phase(table, kind, oid, data, mixed, "same SQL type rewrite")
                command("BEGIN", "BEGIN")
                query("binary", "UPDATE " + table + " SET v=" + literal(kind, 999998) +
                      " WHERE id=3 RETURNING " + projection, fields(oid, mixed),
                      result_rows(kind, [(3, 999998)], mixed, "binary"), "UPDATE 1")
                command("ROLLBACK", "ROLLBACK")
                phase(table, kind, oid, data, mixed, "rollback preserves exact datum")
                tables.append((table, kind, oid, data, mixed))
        command("CREATE TABLE micro_precision(a timestamp(3),b timestamptz(3),c time(3))", "CREATE TABLE")
        precision_fields = [("a", 1114, 8, 3), ("b", 1184, 8, 3), ("c", 1083, 8, 3)]
        for mode, value, rounded, time_in, time_out in (
                ("simple", -500, -1000, 86399999500, 86400000000),
                ("text", 500, 1000, 500, 1000),
                ("binary", -1500, -2000, 1500, 2000)):
            query(mode, "INSERT INTO micro_precision VALUES(" + literal("timestamp", value) + "," +
                  literal("timestamptz", value) + "," + literal("time", time_in) + ") RETURNING a,b,c",
                  precision_fields, [[cell("timestamp", rounded, mode), cell("timestamptz", rounded, mode),
                                      cell("time", time_out, mode)]], "INSERT 0 1")
        command("CREATE TABLE micro_parent(v timestamp PRIMARY KEY)", "CREATE TABLE")
        command("INSERT INTO micro_parent VALUES('2000-01-01 00:00:00.000001')", "INSERT 0 1")
        command("INSERT INTO micro_parent VALUES('2000-01-01 00:00:00.000002')", "INSERT 0 1")
        command("INSERT INTO micro_parent VALUES('2000-01-01 00:00:00.0000010')", state="23505")
        command("CREATE TABLE micro_child(v timestamp REFERENCES micro_parent(v))", "CREATE TABLE")
        command("INSERT INTO micro_child VALUES('2000-01-01 00:00:00.000001')", "INSERT 0 1")
        command("INSERT INTO micro_child VALUES('2000-01-01 00:00:00.000003')", state="23503")
        command("DELETE FROM micro_parent WHERE v='2000-01-01 00:00:00.000001'", state="23503")
        command("CREATE TABLE micro_zones(id integer,v timestamptz)", "CREATE TABLE")
        for ident, zone in enumerate(("America/New_York", "Asia/Kathmandu"), 1):
            command("SET TIME ZONE '" + zone + "'", "SET")
            point = datetime.datetime(2024, 7, 1, 12, 0, 0, 123456, tzinfo=ZoneInfo(zone))
            delta = point.astimezone(datetime.timezone.utc) - epoch
            value = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
            local = point.strftime("%Y-%m-%d %H:%M:%S") + ".123456"
            minutes = int(point.utcoffset().total_seconds()) // 60
            sign = "+" if minutes >= 0 else "-"
            h, m = divmod(abs(minutes), 60)
            shown = (local + sign + "%02d" % h + (":%02d" % m if m else "")).encode()
            for mode in modes:
                query(mode, "INSERT INTO micro_zones VALUES(" + str(ident) + ",'" + local + "') RETURNING id,v",
                      fields(1184, False),
                      [[struct.pack("!i", ident) if mode == "binary" else str(ident).encode(),
                        cell("timestamptz", value, mode) if mode == "binary" else shown]], "INSERT 0 1")
        command("SET TIME ZONE 'UTC'", "SET")
        restart()
        for table, kind, oid, data, mixed in tables:
            phase(table, kind, oid, data, mixed, "cold persisted heap and indexes")
        print("TEMPORAL_HEAP_MICROSECONDS_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == (412 if reference else 408) and not failures, (checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                if schema:
                    client.simple_query(sock, "DROP SCHEMA " + schema + " CASCADE")
            finally:
                sock.close()


if __name__ == "__main__":
    main()
