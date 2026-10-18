"""TimeZone SET separators in Simple/Extended query; actual PostgreSQL 18 oracle."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("timezone_set_runner", root / "tests/compat/pg_diff_runner.py")
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

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print("TIMEZONE_SET_TO", checks, label, "match", good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def set_zone(sql, extended, state=None):
        if extended:
            packet = client.typed(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
            packet += client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
            packet += client.typed(b"D", b"P\0") + client.typed(b"E", b"\0" + struct.pack("!I", 0))
            sock.sendall(packet + client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        kinds = [kind for kind, _ in messages]
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", b"I") and result[1] == state and result[0] == []
        good = good and not any(kind in (b"T", b"D", b"N") for kind in kinds)
        if extended:
            good = good and kinds.count(b"1") == kinds.count(b"2") == 1 and kinds.count(b"n") == 1
        if state:
            good = good and kinds.count(b"E") == 1 and b"C" not in kinds
        else:
            good = good and b"E" not in kinds and [body for kind, body in messages if kind == b"C"] == [b"SET\0"]
        report(good, ("Extended " if extended else "Simple ") + sql, (result, kinds))

    def show_zone(expected):
        messages = client.simple_query(sock, "SHOW TimeZone")
        result = runner.decode_wire_result(messages, include_types=True)
        good = messages[-1] == (b"Z", b"I") and result[1] is None
        good = good and result[0] == [[expected]] and result[3] == ["TimeZone"] and result[5] == [25]
        good = good and [body for kind, body in messages if kind == b"C"] == [b"SHOW\0"]
        report(good, "SHOW expected=" + expected, result)

    try:
        forms = ("SET TimeZone TO {}", "set timezone\tto\n{}", "SET TimeZone = {}", "SET TIME ZONE {}",
                 "SET SESSION TimeZone TO {}", "set\tSESSION\tTimeZone\tto\n{}",
                 "SET SESSION TimeZone = {}", "SET SESSION TIME ZONE {}")
        for zone in ("UTC", "Asia/Shanghai", "America/New_York", "Asia/Kathmandu",
                     "Pacific/Kiritimati", "Europe/Berlin"):
            for form in forms:
                for extended in (False, True):
                    set_zone(form.format("'" + zone + "'"), extended)
                    show_zone(zone)
        # TO within a quoted datum is not a separator. Every failed SET
        # must preserve the previous session value and ready/idle recovery.
        for bad in ("TO UTC", "to Europe/Berlin", "Invalid/No_Such_Zone", ""):
            for prefix in ("SET TimeZone TO ", "SET SESSION TimeZone TO "):
                for extended in (False, True):
                    set_zone(prefix + "'" + bad + "'", extended, state="22023")
                    show_zone("Europe/Berlin")
        print("TIMEZONE_SET_TO_COMPLETE", checks, "FAILURES", len(failures), flush=True)
        assert checks == 224 and not failures, (checks, failures)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == "__main__":
    main()
